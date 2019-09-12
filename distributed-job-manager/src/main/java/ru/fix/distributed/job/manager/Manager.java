package ru.fix.distributed.job.manager;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.aggregating.profiler.Profiler;
import ru.fix.distributed.job.manager.model.JobId;
import ru.fix.distributed.job.manager.model.WorkItem;
import ru.fix.distributed.job.manager.model.WorkerItem;
import ru.fix.distributed.job.manager.model.ZookeeperState;
import ru.fix.distributed.job.manager.strategy.AssignmentStrategy;
import ru.fix.distributed.job.manager.util.ZkTreePrinter;
import ru.fix.dynamic.property.api.DynamicProperty;
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

    private static final int ASSIGNMENT_COMMIT_RETRIES_COUNT = 1;

    private final CuratorFramework curatorFramework;
    private final JobManagerPaths paths;
    private final DynamicProperty<Boolean> printTree;
    private final AssignmentStrategy assignmentStrategy;

    private PathChildrenCache workersAliveChildrenCache;

    private final ExecutorService managerThread;
    private volatile LeaderLatch leaderLatch;
    private final String serverId;

    Manager(CuratorFramework curatorFramework,
            String rootPath,
            AssignmentStrategy assignmentStrategy,
            String serverId,
            Profiler profiler,
            DynamicProperty<Boolean> printTree) {
        this.managerThread = NamedExecutors.newSingleThreadPool("distributed-manager-thread", profiler);
        this.curatorFramework = curatorFramework;
        this.paths = new JobManagerPaths(rootPath);
        this.assignmentStrategy = assignmentStrategy;
        this.printTree = printTree;
        this.leaderLatch = initLeaderLatch();
        this.workersAliveChildrenCache = new PathChildrenCache(
                curatorFramework,
                paths.getWorkersAlivePath(),
                false);
        this.serverId = serverId;
    }

    public void start() throws Exception {
        workersAliveChildrenCache.getListenable().addListener((client, event) -> {
            log.info("sid={} workersAliveChildrenCache event={}",
                    serverId,
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
                    log.warn("sid={} Invalid event type {}", serverId, event.getType());
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
                log.info("sid={} initLeaderLatch Became a leader", serverId);
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
        if (printTree.get()) {
            log.info("sid={} tree before rebalance: \n {}", serverId, buildZkTreeDump());
        }

        try {
            TransactionalClient.tryCommit(
                    curatorFramework,
                    1,
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

        if (printTree.get()) {
            log.info("sid={} tree after rebalance: \n {}", serverId, buildZkTreeDump());
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

    @SuppressWarnings("squid:S3776")
    private void assignWorkPools(ZookeeperGlobalState globalState, TransactionalClient transaction) throws Exception {

        ZookeeperState currentState = generateCurrentState(
                globalState.getAvailableState(), globalState.getPreviousState()
        );
        Map<JobId, List<WorkItem>> itemsToAssign = generateItemsToAssign(
                globalState.getAvailableState(), currentState
        );

        log.info("Available state before rebalance: " + globalState.getAvailableState().toString());
        log.info("Previous state before rebalance: " + globalState.getPreviousState().toString());
        log.info("Current state before rebalance: " + currentState.toString());
        log.info("Items to assign: " + itemsToAssign.toString());

        ZookeeperState newAssignmentState = assignmentStrategy.reassignAndBalance(
                globalState.getAvailableState(),
                globalState.getPreviousState(),
                currentState,
                generateItemsToAssign(globalState.getAvailableState(), currentState)
        );

        log.info("New assignment after rebalance: " + newAssignmentState);

        rewriteZookeeperNodes(newAssignmentState, transaction);
    }

    private void rewriteZookeeperNodes(ZookeeperState newAssignmentState, TransactionalClient transaction) throws Exception {

        removePreviousAssignedWorkPools(transaction);

        for (Map.Entry<WorkerItem, List<WorkItem>> worker : newAssignmentState.entrySet()) {
            String workerId = worker.getKey().getId();
            List<WorkItem> workItems = worker.getValue();

           /* if (transaction.checkPath(paths.getAssignedWorkPooledJobsPath(workerId)) == null) {
                transaction.createPath(paths.getAssignedWorkPooledJobsPath(workerId));
            }*/

            for (WorkItem workItem : workItems) {

                String newPath = ZKPaths.makePath(
                        paths.getAssignedWorkPooledJobsPath(workerId, workItem.getJobId()),
                        JobManagerPaths.WORK_POOL,
                        workItem.getId()
                );
                try {
                    /*createPathIfNotExists(transaction, paths.getAssignedWorkPooledJobsPath(workerId, workItem.getJobId()));
                    createPathIfNotExists(transaction, paths.getAssignedWorkPoolPath(workerId, workItem.getJobId()));
                    createPathIfNotExists(transaction, newPath);*/
                    curatorFramework.create().creatingParentsIfNeeded().forPath(newPath);
                } catch (KeeperException e) {
                    log.warn("Exception while path creating: ", e);
                }
            }
        }
    }

    private void createPathIfNotExists(TransactionalClient transaction, String path) throws Exception {
        if (curatorFramework.checkExists().forPath(path) != null) {
            transaction.deletePath(path);
        }
        transaction.createPath(path);
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
            log.info("sid={} Remove assignment on dead worker {}",
                    serverId,
                    workerAssignedJobsPath);
            try {
                ZKPaths.deleteChildren(curatorFramework.getZookeeperClient().getZooKeeper(),
                        workerAssignedJobsPath, false);
            } catch (KeeperException.NoNodeException e) {
                log.info("Node was already deleted", e);
            }

        }
    }

    private ZookeeperGlobalState getZookeeperGlobalState() throws Exception {
        ZookeeperState availableState = new ZookeeperState();
        ZookeeperState currentState = new ZookeeperState();

        List<String> workersRoots = curatorFramework.getChildren()
                .forPath(paths.getWorkersPath());

        for (String worker : workersRoots) {
            if (curatorFramework.checkExists().forPath(paths.getWorkerAliveFlagPath(worker)) == null) {
                continue;
            }

            if (curatorFramework.checkExists().forPath(paths.getAvailableWorkPooledJobPath(worker)) == null) {
                continue;
            }
            List<String> availableJobIds = curatorFramework.getChildren()
                    .forPath(paths.getAvailableWorkPooledJobPath(worker));

            List<WorkItem> workPool = new ArrayList<>();
            for (String availableJobId : availableJobIds) {
                List<String> workItemsForAvailableJobList = curatorFramework.getChildren()
                        .forPath(paths.getAvailableWorkPoolPath(worker, availableJobId));

                for (String workItem : workItemsForAvailableJobList) {
                    workPool.add(new WorkItem(workItem, availableJobId));
                }
            }
            availableState.put(new WorkerItem(worker), workPool);

            if (curatorFramework.checkExists().forPath(paths.getAssignedWorkPooledJobsPath(worker)) == null) {
                continue;
            }
            List<String> assignedJobIds = curatorFramework.getChildren()
                    .forPath(paths.getAssignedWorkPooledJobsPath(worker));

            List<WorkItem> assignedWorkPool = new ArrayList<>();
            for (String assignedJobId : assignedJobIds) {
                List<String> assignedJobWorkItems = curatorFramework.getChildren()
                        .forPath(paths.getAssignedWorkPoolPath(worker, assignedJobId));

                for (String workItem : assignedJobWorkItems) {
                    workPool.add(new WorkItem(workItem, assignedJobId));
                }
            }
            currentState.put(new WorkerItem(worker), assignedWorkPool);
        }

        return new ZookeeperGlobalState(availableState, currentState);
    }

    /**
     * Add new workers from available state and remove inaccessible workers from current state
     */
    private ZookeeperState generateCurrentState(ZookeeperState available, ZookeeperState current) {
        ZookeeperState newAssignment = new ZookeeperState();

        for (Map.Entry<WorkerItem, List<WorkItem>> worker : current.entrySet()) {
            if (available.containsKey(worker.getKey())) {
                newAssignment.addWorkItems(worker.getKey(), worker.getValue());
            }
        }
        for (Map.Entry<WorkerItem, List<WorkItem>> worker : available.entrySet()) {
            if (!current.containsKey(worker.getKey())) {
                newAssignment.addWorkItems(worker.getKey(), Collections.emptyList());
            }
        }
        return newAssignment;
    }

    private Map<JobId, List<WorkItem>> generateItemsToAssign(
            ZookeeperState availableState,
            ZookeeperState currentState
    ) {
        Map<JobId, List<WorkItem>> workItemsToAssign = new HashMap<>();

        for (Map.Entry<WorkerItem, List<WorkItem>> worker : availableState.entrySet()) {
            for (WorkItem workItem : worker.getValue()) {
                String jobId = workItem.getJobId();

                if (currentState.containsWorkItem(workItem)) {
                    continue;
                }

                if (workItemsToAssign.containsKey(new JobId(jobId))) {
                    List<WorkItem> workItemsOld = new ArrayList<>(workItemsToAssign.get(new JobId(jobId)));
                    workItemsOld.add(workItem);
                    workItemsToAssign.put(new JobId(jobId), workItemsOld);
                } else {
                    workItemsToAssign.put(new JobId(jobId), Collections.singletonList(workItem));
                }
            }
        }
        return workItemsToAssign;
    }

    private static class ZookeeperGlobalState {
        private ZookeeperState availableState;
        private ZookeeperState currentState;

        ZookeeperGlobalState(ZookeeperState availableState, ZookeeperState currentState) {
            this.availableState = availableState;
            this.currentState = currentState;
        }

        public ZookeeperState getAvailableState() {
            return availableState;
        }

        public ZookeeperState getPreviousState() {
            return currentState;
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
