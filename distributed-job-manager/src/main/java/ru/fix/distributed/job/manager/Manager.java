package ru.fix.distributed.job.manager;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.aggregating.profiler.Profiler;
import ru.fix.distributed.job.manager.model.AssignmentState;
import ru.fix.distributed.job.manager.model.JobId;
import ru.fix.distributed.job.manager.model.WorkItem;
import ru.fix.distributed.job.manager.model.WorkerId;
import ru.fix.distributed.job.manager.strategy.AssignmentStrategy;
import ru.fix.distributed.job.manager.util.ZkTreePrinter;
import ru.fix.stdlib.concurrency.threads.NamedExecutors;
import ru.fix.zookeeper.transactional.TransactionalClient;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

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
    private final JobManagerPaths paths;
    private final AssignmentStrategy assignmentStrategy;

    private PathChildrenCache workersAliveChildrenCache;

    private final ExecutorService managerThread;
    private volatile LeaderLatch leaderLatch;
    private final String nodeId;

    Manager(CuratorFramework curatorFramework,
            String rootPath,
            AssignmentStrategy assignmentStrategy,
            String nodeId,
            Profiler profiler
    ) {
        this.managerThread = NamedExecutors.newSingleThreadPool("distributed-manager-thread", profiler);
        this.curatorFramework = curatorFramework;
        this.paths = new JobManagerPaths(rootPath);
        this.assignmentStrategy = assignmentStrategy;
        this.leaderLatch = initLeaderLatch();
        this.workersAliveChildrenCache = new PathChildrenCache(
                curatorFramework,
                paths.getWorkersAlivePath(),
                false);
        this.nodeId = nodeId;
    }

    public void start() throws Exception {
        workersAliveChildrenCache.getListenable().addListener((client, event) -> {
            log.info("nodeId={} workersAliveChildrenCache event={}",
                    nodeId,
                    event.toString());
            switch (event.getType()) {
                case CONNECTION_RECONNECTED:
                case CHILD_UPDATED:
                case CHILD_ADDED:
                case CHILD_REMOVED:
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
                    break;
                case CONNECTION_SUSPENDED:
                    break;
                default:
                    log.warn("nodeId={} Invalid event type {}", nodeId, event.getType());
            }
        });

        leaderLatch.start();
        workersAliveChildrenCache.start();
    }

    private LeaderLatch initLeaderLatch() {
        LeaderLatch latch = new LeaderLatch(curatorFramework, paths.getLeaderLatchPath());
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
                        String assignmentVersionNode = paths.getAssignmentVersion();

                        int version = curatorFramework.checkExists().forPath(assignmentVersionNode).getVersion();

                        removeAssignmentsOnDeadNodes();
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

        rewriteZookeeperNodes(newAssignmentState, transaction);
    }

    private void rewriteZookeeperNodes(AssignmentState newAssignmentState, TransactionalClient transaction) throws Exception {

        removePreviousAssignedWorkPools(transaction);

        for (Map.Entry<WorkerId, HashSet<WorkItem>> worker : newAssignmentState.entrySet()) {
            String workerId = worker.getKey().getId();
            HashSet<WorkItem> workItems = worker.getValue();

            Map<String, List<WorkItem>> jobs = new HashMap<>();
            for (WorkItem workItem : workItems) {
                if (jobs.containsKey(workItem.getJobId().getId())) {
                    List<WorkItem> newWorkItems = new ArrayList<>(jobs.get(workItem.getJobId().getId()));
                    newWorkItems.add(workItem);
                    jobs.put(workItem.getJobId().getId(), newWorkItems);
                } else {
                    jobs.put(workItem.getJobId().getId(), Collections.singletonList(workItem));
                }
            }

            for (Map.Entry<String, List<WorkItem>> job : jobs.entrySet()) {

                transaction.createPath(paths.getAssignedWorkPooledJobsPath(workerId, job.getKey()));
                transaction.createPath(paths.getAssignedWorkPoolPath(workerId, job.getKey()));

                for (WorkItem workItem : job.getValue()) {
                    String newPath = ZKPaths.makePath(
                            paths.getAssignedWorkPooledJobsPath(workerId, job.getKey()),
                            JobManagerPaths.WORK_POOL,
                            workItem.getId()
                    );
                    transaction.createPath(newPath);
                }
            }
        }
    }

    private void removePreviousAssignedWorkPools(TransactionalClient transaction) {
        try {
            List<String> workersRoots = curatorFramework.getChildren()
                    .forPath(paths.getWorkersPath());

            for (String worker : workersRoots) {

                List<String> jobs = curatorFramework.getChildren()
                        .forPath(paths.getAssignedWorkPooledJobsPath(worker));
                for (String job : jobs) {
                    transaction.deletePathWithChildrenIfNeeded(paths.getAssignedWorkPooledJobsPath(worker, job));
                }
            }
        } catch (Exception e) {
            log.warn("Unable remove previous assignment: ", e);
        }
    }

    private void removeAssignmentsOnDeadNodes() throws Exception {
        List<String> workersRoots = curatorFramework.getChildren().forPath(paths.getWorkersPath());

        for (String worker : workersRoots) {
            if (curatorFramework.checkExists().forPath(paths.getWorkerAliveFlagPath(worker)) != null) {
                continue;
            }

            String workerAssignedJobsPath = paths.getAssignedWorkPooledJobsPath(worker);
            log.info("nodeId={} Remove assignment on dead worker {}",
                    nodeId,
                    workerAssignedJobsPath);
            try {
                ZKPaths.deleteChildren(curatorFramework.getZookeeperClient().getZooKeeper(),
                        workerAssignedJobsPath, false);
            } catch (KeeperException.NoNodeException e) {
                log.info("Node was already deleted", e);
            }
        }
    }

    private GlobalAssignmentState getZookeeperGlobalState() throws Exception {
        AssignmentState availableState = new AssignmentState();
        AssignmentState assignedState = new AssignmentState();

        List<String> workersRoots = curatorFramework.getChildren()
                .forPath(paths.getWorkersPath());

        for (String worker : workersRoots) {
            if (curatorFramework.checkExists().forPath(paths.getWorkerAliveFlagPath(worker)) == null) {
                continue;
            }

            List<String> availableJobIds = curatorFramework.getChildren()
                    .forPath(paths.getAvailableWorkPooledJobPath(worker));

            HashSet<WorkItem> workPool = new HashSet<>();
            for (String availableJobId : availableJobIds) {
                List<String> workItemsForAvailableJobList = curatorFramework.getChildren()
                        .forPath(paths.getAvailableWorkPoolPath(worker, availableJobId));

                for (String workItem : workItemsForAvailableJobList) {
                    workPool.add(new WorkItem(workItem, new JobId(availableJobId)));
                }
            }
            availableState.put(new WorkerId(worker), workPool);

            List<String> assignedJobIds = curatorFramework.getChildren()
                    .forPath(paths.getAssignedWorkPooledJobsPath(worker));

            HashSet<WorkItem> assignedWorkPool = new HashSet<>();
            for (String assignedJobId : assignedJobIds) {
                List<String> assignedJobWorkItems = curatorFramework.getChildren()
                        .forPath(paths.getAssignedWorkPoolPath(worker, assignedJobId));

                for (String workItem : assignedJobWorkItems) {
                    assignedWorkPool.add(new WorkItem(workItem, new JobId(assignedJobId)));
                }
            }
            assignedState.put(new WorkerId(worker), assignedWorkPool);
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
}