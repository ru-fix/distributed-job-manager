package ru.fix.distributed.job.manager;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.aggregating.profiler.Profiler;
import ru.fix.distributed.job.manager.model.*;
import ru.fix.distributed.job.manager.strategy.AssignmentStrategy;
import ru.fix.distributed.job.manager.util.ZkTreePrinter;
import ru.fix.stdlib.concurrency.threads.NamedExecutors;
import ru.fix.zookeeper.transactional.TransactionalClient;

import java.util.*;
import java.util.concurrent.*;

/**
 * Only single manager is active on the cluster.
 * Manages job assignments on cluster by modifying assignment section of zookeeper tree.
 *
 * @author Kamil Asfandiyarov
 * @see Worker
 */
class Manager implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Manager.class);
    private static final int ASSIGNMENT_COMMIT_RETRIES_COUNT = 3;

    private final CuratorFramework curatorFramework;
    private final ZkPathsManager paths;
    private final AssignmentStrategy assignmentStrategy;

    private PathChildrenCache workersAliveChildrenCache;
    private PathChildrenCache workPoolChildrenCache;

    private RebalanceEventConsumer rebalanceEventConsumer;

    private final ExecutorService managerThread;
    private volatile LeaderLatch leaderLatch;
    private final String nodeId;

    Manager(CuratorFramework curatorFramework,
            Profiler profiler,
            DistributedJobManagerSettings settings
    ) {
        this.nodeId = settings.getNodeId();
        this.managerThread = NamedExecutors.newSingleThreadPool("distributed-manager-thread", profiler);
        this.curatorFramework = curatorFramework;
        this.paths = new ZkPathsManager(settings.getRootPath());
        this.assignmentStrategy = settings.getAssignmentStrategy();
        this.leaderLatch = initLeaderLatch();
        this.rebalanceEventConsumer = new RebalanceEventConsumer();
        this.workersAliveChildrenCache = new PathChildrenCache(
                curatorFramework,
                paths.aliveWorkers(),
                false);
        this.workPoolChildrenCache = new PathChildrenCache(
                curatorFramework,
                paths.availableWorkPool(),
                false);
    }

    public void start() throws Exception {
        workersAliveChildrenCache.getListenable().addListener(rebalanceEventConsumer);
        workPoolChildrenCache.getListenable().addListener(rebalanceEventConsumer);
        CompletableFuture.runAsync(rebalanceEventConsumer, Executors.newSingleThreadExecutor());
        leaderLatch.start();
        workersAliveChildrenCache.start();
        workPoolChildrenCache.start();
    }

    private LeaderLatch initLeaderLatch() {
        LeaderLatch latch = new LeaderLatch(curatorFramework, paths.leaderLatch());
        latch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                log.info("nodeId={} initLeaderLatch Became a leader", nodeId);
                synchronized (managerThread) {
                    if (managerThread.isShutdown()) {
                        return;
                    }
                    managerThread.execute(Manager.this::reassignAndBalanceTasks);
                }
            }

            @Override
            public void notLeader() {
                //   Do nothing when leadership is lost
            }
        }, managerThread);
        return latch;
    }

    /**
     * Rebalance tasks in tasks tree for all available workers after any failure or workers count change
     */
    private void reassignAndBalanceTasks() {
        if (!curatorFramework.getState().equals(CuratorFrameworkState.STARTED)) {
            log.error("Ignore reassignAndBalanceTasks: curatorFramework is not started");
            return;
        }
        if (!curatorFramework.getZookeeperClient().isConnected()) {
            log.error("Ignore reassignAndBalanceTasks: lost connection to zookeeper");
            return;
        }
        if (log.isTraceEnabled()) {
            log.trace("nodeId={} tree before rebalance: \n {}", nodeId, buildZkTreeDump());
        }

        try {
            TransactionalClient.tryCommit(
                    curatorFramework,
                    ASSIGNMENT_COMMIT_RETRIES_COUNT,
                    transaction -> {
                        String assignmentVersionNode = paths.assignmentVersion();

                        int version = curatorFramework.checkExists().forPath(assignmentVersionNode).getVersion();

                        transaction.checkPathWithVersion(assignmentVersionNode, version);
                        transaction.setData(assignmentVersionNode, new byte[]{});

                        assignWorkPools(getZookeeperGlobalState(), transaction);
                    }
            );

        } catch (Exception e) {
            log.warn("Can't reassign and balance tasks: ", e);
        }

        if (log.isTraceEnabled()) {
            log.trace("nodeId={} tree after rebalance: \n {}", nodeId, buildZkTreeDump());
        }
    }

    private String buildZkTreeDump() {
        try {
            return new ZkTreePrinter(curatorFramework).print(paths.rootPath);
        } catch (Exception ex) {
            log.warn("Failed to build zk tree", ex);
            return "";
        }
    }

    private void assignWorkPools(GlobalAssignmentState globalState, TransactionalClient transaction) throws Exception {
        AssignmentState currentState = new AssignmentState();
        AssignmentState previousState = globalState.getAssignedState();
        AssignmentState availableState = globalState.getAvailableState();
        Map<JobId, Set<WorkerId>> availability = generateAvailability(availableState);

        if (log.isTraceEnabled()) {
            log.trace("Availability before rebalance: " + availability +
                    "\nAvailable state before rebalance: " + availableState);
        }

        AssignmentState newAssignmentState = assignmentStrategy.reassignAndBalance(
                availability,
                previousState,
                currentState,
                generateItemsToAssign(availableState)
        );

        if (log.isTraceEnabled()) {
            log.trace("Previous state before rebalance: " + previousState +
                    "\nNew assignment after rebalance: " + newAssignmentState);
        }

        rewriteZookeeperNodes(previousState, newAssignmentState, transaction);
    }

    /**
     * Add in zk paths of new work items, that contains in newAssignmentState, but doesn't in previousState and
     * remove work items, that contains in previousState, but doesn't in newAssignmentState.
     */
    private void rewriteZookeeperNodes(
            AssignmentState previousState,
            AssignmentState newAssignmentState,
            TransactionalClient transaction
    ) throws Exception {
        removeAssignmentsOnDeadNodes(transaction);

        for (Map.Entry<WorkerId, HashSet<WorkItem>> worker : newAssignmentState.entrySet()) {
            WorkerId workerId = worker.getKey();
            Map<JobId, List<WorkItem>> jobs = itemsToMap(worker.getValue());

            if (curatorFramework.checkExists()
                    .forPath(paths.aliveWorker(workerId.getId())) == null) {
                continue;
            }
            for (Map.Entry<JobId, List<WorkItem>> job : jobs.entrySet()) {
                createIfNotExist(transaction, paths.assignedWorkPool(
                        workerId.getId(), job.getKey().getId())
                );

                for (WorkItem workItem : job.getValue()) {
                    if (!previousState.containsWorkItemOnWorker(workerId, workItem)) {
                        transaction.createPath(paths
                                .assignedWorkItem(workerId.getId(), job.getKey().getId(), workItem.getId()));
                    }
                }
            }
        }

        for (Map.Entry<WorkerId, HashSet<WorkItem>> worker : previousState.entrySet()) {
            WorkerId workerId = worker.getKey();
            Map<JobId, List<WorkItem>> jobs = itemsToMap(worker.getValue());

            for (Map.Entry<JobId, List<WorkItem>> job : jobs.entrySet()) {
                for (WorkItem workItem : job.getValue()) {
                    if (!newAssignmentState.containsWorkItemOnWorker(workerId, workItem)) {
                        transaction.deletePathWithChildrenIfNeeded(paths
                                .assignedWorkItem(workerId.getId(), job.getKey().getId(), workItem.getId()));
                    }
                }
            }
        }
    }

    private void createIfNotExist(TransactionalClient transactionalClient, String path) throws Exception {
        if (curatorFramework.checkExists().forPath(path) == null) {
            transactionalClient.createPath(path);
        }
    }

    private void removeAssignmentsOnDeadNodes(TransactionalClient transaction) throws Exception {
        List<String> workersRoots = curatorFramework.getChildren().forPath(paths.allWorkers());

        for (String worker : workersRoots) {
            if (curatorFramework.checkExists().forPath(paths.aliveWorker(worker)) != null) {
                continue;
            }
            log.info("nodeId={} Remove dead worker {}", nodeId, worker);

            try {
                transaction.deletePathWithChildrenIfNeeded(paths.worker(worker));
            } catch (KeeperException.NoNodeException e) {
                log.info("Node was already deleted", e);
            }
        }
    }

    private Map<JobId, List<WorkItem>> itemsToMap(Set<WorkItem> workItems) {
        Map<JobId, List<WorkItem>> jobs = new HashMap<>();
        for (WorkItem workItem : workItems) {
            jobs.putIfAbsent(workItem.getJobId(), new ArrayList<>());
            jobs.get(workItem.getJobId()).add(workItem);
        }
        return jobs;
    }

    private GlobalAssignmentState getZookeeperGlobalState() throws Exception {
        AssignmentState availableState = new AssignmentState();
        AssignmentState assignedState = new AssignmentState();

        List<String> workersRoots = curatorFramework.getChildren()
                .forPath(paths.allWorkers());

        for (String worker : workersRoots) {
            if (curatorFramework.checkExists().forPath(paths.aliveWorker(worker)) == null) {
                continue;
            }
            List<String> assignedJobIds = curatorFramework.getChildren()
                    .forPath(paths.assignedJobs(worker));

            HashSet<WorkItem> assignedWorkPool = new HashSet<>();
            for (String assignedJobId : assignedJobIds) {
                List<String> assignedJobWorkItems = curatorFramework.getChildren()
                        .forPath(paths.assignedWorkPool(worker, assignedJobId));

                for (String workItem : assignedJobWorkItems) {
                    assignedWorkPool.add(new WorkItem(workItem, new JobId(assignedJobId)));
                }
            }
            assignedState.put(new WorkerId(worker), assignedWorkPool);
        }

        for (String worker : workersRoots) {
            if (curatorFramework.checkExists().forPath(paths.aliveWorker(worker)) == null) {
                continue;
            }

            List<String> availableJobIds = curatorFramework.getChildren()
                    .forPath(paths.availableJobs(worker));

            HashSet<WorkItem> availableWorkPool = new HashSet<>();
            for (String availableJobId : availableJobIds) {
                List<String> workItemsForAvailableJobList = curatorFramework.getChildren()
                        .forPath(paths.availableWorkPool(availableJobId));

                for (String workItem : workItemsForAvailableJobList) {
                    availableWorkPool.add(new WorkItem(workItem, new JobId(availableJobId)));
                }
            }
            availableState.put(new WorkerId(worker), availableWorkPool);
        }

        return new GlobalAssignmentState(availableState, assignedState);
    }

    private Map<JobId, Set<WorkerId>> generateAvailability(AssignmentState assignmentState) {
        Map<JobId, Set<WorkerId>> availability = new HashMap<>();

        for (Map.Entry<WorkerId, HashSet<WorkItem>> workerEntry : assignmentState.entrySet()) {
            for (WorkItem workItem : workerEntry.getValue()) {
                availability.computeIfAbsent(
                        workItem.getJobId(), state -> new HashSet<>()
                ).add(workerEntry.getKey());
            }
        }

        return availability;
    }

    private HashSet<WorkItem> generateItemsToAssign(AssignmentState assignmentState) {
        HashSet<WorkItem> itemsToAssign = new HashSet<>();

        for (Map.Entry<WorkerId, HashSet<WorkItem>> workerEntry : assignmentState.entrySet()) {
            itemsToAssign.addAll(workerEntry.getValue());
        }

        return itemsToAssign;
    }

    private static class GlobalAssignmentState {
        private AssignmentState availableState;
        private AssignmentState assignedState;

        GlobalAssignmentState(
                AssignmentState availableState,
                AssignmentState assignedState
        ) {
            this.availableState = availableState;
            this.assignedState = assignedState;
        }

        AssignmentState getAvailableState() {
            return availableState;
        }

        AssignmentState getAssignedState() {
            return assignedState;
        }
    }

    @Override
    public void close() throws Exception {
        long managerStopTime = System.currentTimeMillis();
        log.info("Closing DJM manager entity...");

        workersAliveChildrenCache.close();
        workPoolChildrenCache.close();
        if (LeaderLatch.State.STARTED == leaderLatch.getState()) {
            leaderLatch.close();
        }
        synchronized (managerThread) {
            managerThread.shutdown();
        }
        if (!managerThread.awaitTermination(3, TimeUnit.MINUTES)) {
            log.error("Failed to wait manager thread pool termination");
            managerThread.shutdownNow();
        }
        log.info("DJM manager was closed. Took {} ms", System.currentTimeMillis() - managerStopTime);
    }


    private class RebalanceEventConsumer implements Runnable, PathChildrenCacheListener {
        private static final long CHECK_SHUTDOWN_PERIOD_MS = 1_000L;

        private BlockingQueue<PathChildrenCacheEvent> rebalanceEventsQueue;

        RebalanceEventConsumer() {
            rebalanceEventsQueue = new LinkedBlockingQueue<>();
        }

        @Override
        public void run() {
            while (true) {
                awaitRebalanceEventOrShutdown();

                synchronized (managerThread) {
                    if (managerThread.isShutdown()) {
                        return;
                    }
                    managerThread.execute(() -> {
                        if (leaderLatch.hasLeadership()) {
                            reassignAndBalanceTasks();
                        }
                    });
                }
            }
        }

        private void awaitRebalanceEventOrShutdown() {
            try {
                while (rebalanceEventsQueue.poll(CHECK_SHUTDOWN_PERIOD_MS, TimeUnit.MILLISECONDS) == null) {
                    synchronized (managerThread) {
                        if (managerThread.isShutdown()) {
                            break;
                        }
                    }
                }
            } catch (InterruptedException e) {
                log.error("waiting rebalance event was interrupted", e);
            } finally {
                rebalanceEventsQueue.clear();
            }
        }

        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
            log.trace("nodeId={} handleRebalanceEvent event={}", nodeId, event);
            switch (event.getType()) {
                case CONNECTION_RECONNECTED:
                case CHILD_UPDATED:
                case CHILD_ADDED:
                case CHILD_REMOVED:
                    rebalanceEventsQueue.put(event);
                    break;
                case CONNECTION_SUSPENDED:
                    break;
                default:
                    log.warn("nodeId={} Invalid event type {}", nodeId, event.getType());
            }
        }

    }
}