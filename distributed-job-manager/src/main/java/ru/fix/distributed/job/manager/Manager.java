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
import ru.fix.distributed.job.manager.model.distribution.JobId;
import ru.fix.distributed.job.manager.model.distribution.JobState;
import ru.fix.distributed.job.manager.model.distribution.WorkItem;
import ru.fix.distributed.job.manager.model.distribution.WorkerItem;
import ru.fix.distributed.job.manager.strategy.AssignmentStrategy;
import ru.fix.distributed.job.manager.util.ZkTreePrinter;
import ru.fix.dynamic.property.api.DynamicProperty;
import ru.fix.stdlib.concurrency.threads.NamedExecutors;
import ru.fix.zookeeper.transactional.TransactionalClient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Only single manager is active on the cluster.
 * Manages job assignments on cluster by modifying assignment section of zookeeper tree.
 *
 * @author Kamil Asfandiyarov
 * @see Worker
 */
class Manager implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Manager.class);

    private static final int ASSIGNMENT_COMMIT_RETRIES_COUNT = 10;

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
                /**
                 * Do nothing when leadership is lost
                 */
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
            TransactionalClient.tryCommit(curatorFramework, ASSIGNMENT_COMMIT_RETRIES_COUNT, transactionalClient -> {
                // read-up version
                String assignmentVersionNode = paths.getAssignmentVersion();
                int version = curatorFramework.checkExists().forPath(assignmentVersionNode).getVersion();

                // read-up required values
                removeAssignmentsOnDeadNodes();
                JobsSnapshot jobsSnapshot = takeJobsSnapshot();

                transactionalClient.checkPathWithVersion(assignmentVersionNode, version);
                transactionalClient.setData(assignmentVersionNode, new byte[]{});
                assignWorkPools(transactionalClient, jobsSnapshot);
            });
        } catch (Exception e) {
            log.error("Failed to perform assignment", e);
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
    private void assignWorkPools(TransactionalClient transactionalClient, JobsSnapshot jobsSnapshot) throws Exception {
        Map<JobId, JobState> availabilityState = jobsSnapshot.getAvailabilityState();
        Map<JobId, JobState> assignmentState = jobsSnapshot.getAssignmentState();

        // here we call method which take all zookeeper availability and current assignment states,
        // then we call injected assignment strategy
        /*for (Map.Entry<JobId, JobState> jobAvailability : availabilityState.entrySet()) {
            JobId jobId = jobAvailability.getKey();
            Set<WorkerItem> currentAssignment = assignmentState.get(jobId).getWorkers();


            if (wpAssignmentStrategy == null) {
                throw new IllegalStateException("Got null assignment strategy for job " + jobId.getId());
            }
            JobState newJobState = wpAssignmentStrategy.reassignAndBalance(jobAvailability.getValue(),
                    assignmentState.get(jobId));

            // cleanup empty workers
            Set<WorkerItem> newAssignment = newJobState.getWorkers().stream()
                    .filter(w -> !w.getWorkPools().isEmpty()).collect(Collectors.toSet());

            // whole workers removals
            Set<WorkerItem> workersToRemove = currentAssignment.stream()
                    .filter(v -> !newAssignment.contains(v)).collect(Collectors.toSet());
            for (WorkerItem workerItem : workersToRemove) {
                // remove path [worker/assigned/job/...]
                String jobPoolForWorkerPath =
                        paths.getAssignedWorkPooledJobsPath(workerItem.getId(), jobId.getId());
                transactionalClient.deletePathWithChildrenIfNeeded(jobPoolForWorkerPath);
            }

            // whole workers additions
            Set<WorkerItem> workersToAdd = newAssignment.stream()
                    .filter(v -> !currentAssignment.contains(v)).collect(Collectors.toSet());
            for (WorkerItem workerItem : workersToAdd) {
                // add path [worker/assigned/job/...]
                String jobPoolForWorkerPath =
                        paths.getAssignedWorkPooledJobsPath(workerItem.getId(), jobId.getId());
                String jobPoolForWorkerPathWithWP = ZKPaths.makePath(jobPoolForWorkerPath, JobManagerPaths.WORK_POOL);
                transactionalClient.createPath(jobPoolForWorkerPath);
                transactionalClient.createPath(jobPoolForWorkerPathWithWP);
                for (WorkItem workItem : workerItem.getWorkPools()) {
                    transactionalClient.createPath(ZKPaths.makePath(jobPoolForWorkerPathWithWP, workItem.getId()));
                }
            }

            // work pools adjustment
            Map<WorkerItem, WorkerItem> workPoolsAdjustmentMap = new HashMap<>(); // current -> new
            currentAssignment.stream()
                    .filter(newAssignment::contains)
                    .forEach(v -> workPoolsAdjustmentMap.put(v, v));
            newAssignment.forEach(v -> {
                WorkerItem currentAssignmentItem = workPoolsAdjustmentMap.get(v);
                if (currentAssignmentItem != null) {
                    workPoolsAdjustmentMap.put(currentAssignmentItem, v);
                }
            });

            for (Map.Entry<WorkerItem, WorkerItem> workerItemsEntry : workPoolsAdjustmentMap.entrySet()) {
                Set<WorkItem> currentWorkerItems = workerItemsEntry.getKey().getWorkPools();
                Set<WorkItem> newWorkerItems = workerItemsEntry.getValue().getWorkPools();

                // work pool removals
                Set<WorkItem> workItemsToRemove = currentWorkerItems.stream()
                        .filter(v -> !newWorkerItems.contains(v))
                        .collect(Collectors.toSet());
                for (WorkItem workItem : workItemsToRemove) {
                    // remove work pool item
                    String workPoolPath = ZKPaths.makePath(
                            paths.getAssignedWorkPooledJobsPath(workerItemsEntry.getKey().getId(),
                                    jobId.getId()),
                            JobManagerPaths.WORK_POOL,
                            workItem.getId());
                    transactionalClient.deletePath(workPoolPath);
                }

                // work pool additions
                Set<WorkItem> workItemsToAdd = newWorkerItems.stream()
                        .filter(v -> !currentWorkerItems.contains(v)).collect(Collectors.toSet());
                for (WorkItem workItem : workItemsToAdd) {
                    // add work pool item
                    String workPoolPath = ZKPaths.makePath(
                            paths.getAssignedWorkPooledJobsPath(workerItemsEntry.getKey().getId(),
                                    jobId.getId()),
                            JobManagerPaths.WORK_POOL,
                            workItem.getId());
                    transactionalClient.createPath(workPoolPath);
                }
            }
        }*/
    }

    private void removeAssignmentsOnDeadNodes() throws Exception {
        // retrieve workers list
        List<String> workersRoots = curatorFramework.getChildren().forPath(paths.getWorkersPath());
        for (String worker : workersRoots) {
            if (curatorFramework.checkExists().forPath(paths.getWorkerAliveFlagPath(worker)) == null) {
                String workerAssignedJobsPath = paths.getAssignedWorkPooledJobsPath(worker);
                log.info("sid={} Remove assignment on dead worker {}",
                        serverId,
                        workerAssignedJobsPath);
                try {
                    ZKPaths.deleteChildren(curatorFramework.getZookeeperClient().getZooKeeper(),
                            workerAssignedJobsPath, false);
                } catch (KeeperException.NoNodeException e) {
                    log.trace("Node was already deleted", e);
                }
            }
        }
    }

    private JobsSnapshot takeJobsSnapshot() throws Exception {
        JobsSnapshot jobsState = new JobsSnapshot();

        // retrieve workers list
        List<String> workersRoots = curatorFramework.getChildren().forPath(paths.getWorkersPath());
        for (String worker : workersRoots) {
            if (curatorFramework.checkExists().forPath(paths.getWorkerAliveFlagPath(worker)) == null) {
                continue;
            }

            log.trace("Detect live worker {}", paths.getWorkerAliveFlagPath(worker));

            // availability
            if (curatorFramework.checkExists().forPath(paths.getAvailableWorkPooledJobPath(worker)) != null) {
                List<String> availableJobIds = curatorFramework.getChildren()
                        .forPath(paths.getAvailableWorkPooledJobPath(worker));
                for (String availableJobId : availableJobIds) {
                    JobState availabilityState = jobsState.getAvailabilityState()
                            .computeIfAbsent(new JobId(availableJobId), v -> new JobState());

                    WorkerItem availableWorkerItem = new WorkerItem(worker);
                    availabilityState.getWorkers().add(availableWorkerItem);

                    List<String> localWorkPool = curatorFramework.getChildren()
                            .forPath(paths.getAvailableWorkPoolPath(worker, availableJobId));
                    availableWorkerItem.getWorkPools().addAll(localWorkPool.stream()
                            .map(WorkItem::new).collect(Collectors.toSet()));

                    // create assignment state for the same worker if doesn't exist
                    jobsState.getAssignmentState().computeIfAbsent(new JobId(availableJobId), v -> new JobState());
                }
            }

            // assignment
            if (curatorFramework.checkExists().forPath(paths.getAssignedWorkPooledJobsPath(worker)) != null) {
                List<String> assignedJobIds = curatorFramework.getChildren()
                        .forPath(paths.getAssignedWorkPooledJobsPath(worker));
                for (String assignedJobId : assignedJobIds) {
                    JobState assignmentState = jobsState.getAssignmentState().get(new JobId(assignedJobId));

                    WorkerItem assignedWorkerItem = new WorkerItem(worker);
                    assignmentState.getWorkers().add(assignedWorkerItem);

                    List<String> localWorkPool = curatorFramework.getChildren()
                            .forPath(paths.getAssignedWorkPoolPath(worker, assignedJobId));
                    assignedWorkerItem.getWorkPools().addAll(localWorkPool.stream()
                            .map(WorkItem::new).collect(Collectors.toSet()));
                }
            }
        }
        return jobsState;
    }

    private static class JobsSnapshot {
        private Map<JobId, JobState> availabilityState = new HashMap<>();
        private Map<JobId, JobState> assignmentState = new HashMap<>();

        public Map<JobId, JobState> getAssignmentState() {
            return assignmentState;
        }

        public Map<JobId, JobState> getAvailabilityState() {
            return availabilityState;
        }

        @Override
        public String toString() {
            return "JobsSnapshot{" +
                    "availabilityState=" + availabilityState +
                    ",assignmentState=" + assignmentState +
                    '}';
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
