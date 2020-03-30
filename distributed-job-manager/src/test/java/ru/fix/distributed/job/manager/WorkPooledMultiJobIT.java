package ru.fix.distributed.job.manager;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.aggregating.profiler.AggregatingProfiler;
import ru.fix.distributed.job.manager.assertion.RetryAssert;
import ru.fix.distributed.job.manager.model.DistributedJobManagerSettings;
import ru.fix.distributed.job.manager.strategy.AssignmentStrategies;
import ru.fix.dynamic.property.api.DynamicProperty;
import ru.fix.stdlib.socket.proxy.ProxySocket;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.mockito.Mockito.*;
import static ru.fix.distributed.job.manager.StubbedMultiJob.getJobId;

/**
 * @author Ayrat Zulkarnyaev
 */
public class WorkPooledMultiJobIT extends AbstractJobManagerTest {
    private static final int DEFAULT_TIMEOUT = 15_000;

    private static final Logger logger = LoggerFactory.getLogger(WorkPooledMultiJobIT.class);

    @Test
    public void shouldAddNewAvailableWorkPool() throws Exception {
        final String nodeId = "common-worker-1";
        try (
                CuratorFramework curator = zkTestingServer.createClient();
                DistributedJobManager jobManager1 = createNewJobManager(nodeId, curator)
        ) {

            RetryAssert.assertTrue(
                    () -> "Wait for assignment common-worker-1 --> work-item-1.1" + printZkTree
                            (JOB_MANAGER_ZK_ROOT_PATH),
                    () -> {
                        String jobId = getJobId(1);
                        Stat commonWorkerPoolChecker = zkTestingServer.getClient().checkExists()
                                .forPath(paths.availableWorkItem(jobId, "work-item-1.1"));
                        return commonWorkerPoolChecker != null;
                    },
                    30_000);
        }
    }

    @Test
    public void shouldDistributeCommonJobs() throws Exception {
        final String[] nodeIds = {"distr-worker-1", "distr-worker-2", "distr-worker-3"};
        try (
                CuratorFramework curator = zkTestingServer.createClient();
                DistributedJobManager jobManager1 = createNewJobManager(nodeIds[0], curator);
                DistributedJobManager jobManager2 = createNewJobManager(nodeIds[1], curator);
                DistributedJobManager jobManager3 = createNewJobManager(nodeIds[2], curator)
        ) {
            String searchedWorkItem = "work-item-1.1";
            RetryAssert.assertTrue("Wait for assignment work-item-1.1 to any worker",
                    () -> {
                        // Work pool contains 3 work items. Then every distributed job should contains 1 work item.
                        for (String nodeId : nodeIds) {
                            String assignedWorkpoolPath = paths.assignedWorkPool(nodeId, getJobId(1));
                            if (curator.checkExists().forPath(assignedWorkpoolPath) != null) {
                                List<String> workPool = curator.getChildren().forPath(assignedWorkpoolPath);
                                if (workPool.contains(searchedWorkItem)) {
                                    return true;
                                }
                            }
                        }
                        return false;
                    }, 30_000);
        }
    }

    @Test
    @Disabled("При повторном создании ProxySocket порт оказывается уже занятым")
    public void shouldDistributeCommonJobs_AfterReconnect() throws Exception {
        String worker1 = "distr-worker-1";
        String worker2 = "distr-worker-2";
        int sessionTimeout = 6_000;
        try (
                ProxySocket proxySocket = new ProxySocket("localhost",
                        zkTestingServer.getPort(), 0, Executors.newFixedThreadPool(15));
                CuratorFramework proxiedCurator = zkTestingServer.createClient("127.0.0.1:" +
                        proxySocket.getPort(), sessionTimeout, 5_000, 5_000);
                DistributedJobManager jobManager1 = createNewJobManager(worker1, proxiedCurator);
                DistributedJobManager jobManager2 = createNewJobManager(worker2, zkTestingServer.getClient())
        ) {
            RetryAssert.assertTrue("All jobs assigned between workers",
                    () -> {
                        if (proxiedCurator.checkExists().forPath(paths.assignedWorkPool(worker1, getJobId(1)))
                                != null &&
                                proxiedCurator.checkExists().forPath(paths.assignedWorkPool(worker2, getJobId
                                        (1))) != null) {
                            Set<String> workItems = getWorkItems(1);
                            List<String> workPool1 = proxiedCurator.getChildren().forPath(paths
                                    .assignedWorkPool(worker1, getJobId(1)));
                            List<String> workPool2 = proxiedCurator.getChildren().forPath(paths
                                    .assignedWorkPool(worker2, getJobId(1)));
                            workItems.removeAll(workPool1);
                            workItems.removeAll(workPool2);
                            return workItems.isEmpty();
                        }
                        return false;
                    }, 30_000);

            proxySocket.close();
            logger.info("Closed proxy to emulate network issue. Curator connection timeout is {}. Waiting {} + 10_000" +
                    " ms until connection lost.", sessionTimeout, 3_000);
            RetryAssert.assertTrue("Curator is disconnected",
                    () -> !proxiedCurator.getZookeeperClient().getZooKeeper().getState().isConnected(),
                    sessionTimeout + 3_000);

            RetryAssert.assertTrue("All jobs assigned to second worker",
                    () -> {
                        if (zkTestingServer.getClient().checkExists().forPath(paths.assignedWorkPool(worker2,
                                getJobId(1))) != null) {
                            Set<String> workItems = getWorkItems(1);

                            List<String> workPool1 = new ArrayList<>();
                            if (zkTestingServer.getClient().checkExists().forPath(paths.assignedWorkPool
                                    (worker1, getJobId(1))) != null) {
                                try {
                                    workPool1.addAll(zkTestingServer.getClient().getChildren().forPath(paths
                                            .assignedWorkPool(worker1, getJobId(1))));
                                } catch (KeeperException.NoNodeException e) {
                                    // ignore this exception here
                                }
                            }

                            List<String> workPool2 = zkTestingServer.getClient().getChildren().forPath(paths
                                    .assignedWorkPool(worker2, getJobId(1)));
                            return workPool1.isEmpty() && workPool2.containsAll(workItems);
                        }
                        return false;
                    }, 30_000);

            logger.info("Restored proxy on the same port {}. Curator could reconnect now.", proxySocket.getPort());

            RetryAssert.assertTrue("Curator is connected",
                    () -> {
                        try {
                            return proxiedCurator.getZookeeperClient().getZooKeeper().getState().isConnected();
                        } catch (Exception e) {
                            return false;
                        }
                    }, DEFAULT_TIMEOUT);

            RetryAssert.assertTrue(
                    () -> "Jobs was again distributed between workers (and worker1 received at least " +
                            "one pool back) " + printZkTree(JOB_MANAGER_ZK_ROOT_PATH),
                    () -> {
                        if (proxiedCurator.checkExists().forPath(paths.assignedWorkPool(worker1, getJobId(1)))
                                != null &&
                                proxiedCurator.checkExists().forPath(paths.assignedWorkPool(worker2, getJobId
                                        (1))) != null) {
                            Set<String> workItems = getWorkItems(1);
                            List<String> workPool1 = proxiedCurator.getChildren().forPath(paths
                                    .assignedWorkPool(worker1, getJobId(1)));
                            List<String> workPool2 = proxiedCurator.getChildren().forPath(paths
                                    .assignedWorkPool(worker2, getJobId(1)));
                            workItems.removeAll(workPool1);
                            workItems.removeAll(workPool2);
                            return workItems.isEmpty() && !workPool1.isEmpty();
                        }
                        return false;
                    }, 50_000);
        }
    }

    @Test
    public void shouldUnevenDistribute() throws Exception {
        final String[] nodeIds = {"uneven-worker-1", "uneven-worker-2"};
        try (
                CuratorFramework curator = zkTestingServer.createClient();
                DistributedJobManager jobManager1 = createNewJobManager(nodeIds[0], curator);
                DistributedJobManager jobManager2 = createNewJobManager(nodeIds[1], curator)
        ) {
            RetryAssert.assertTrue(
                    () -> "All work pool should distribute to workers " + printZkTree(JOB_MANAGER_ZK_ROOT_PATH),
                    () -> {
                        String pathForWorker1 = paths.assignedWorkPool(nodeIds[0], getJobId(1));
                        String pathForWorker2 = paths.assignedWorkPool(nodeIds[1], getJobId(1));

                        if (curator.checkExists().forPath(pathForWorker1) != null && curator.checkExists().forPath
                                (pathForWorker2) != null) {
                            List<String> firstWorkPool =
                                    curator.getChildren().forPath(pathForWorker1);

                            List<String> secondWorkPool =
                                    curator.getChildren().forPath(pathForWorker2);
                            Set<String> mergedWorkPool = new HashSet<>();

                            mergedWorkPool.addAll(firstWorkPool);
                            mergedWorkPool.addAll(secondWorkPool);

                            Set<String> commonWorkPool = getWorkItems(1);
                            return firstWorkPool.size() > 0
                                    && secondWorkPool.size() > 0
                                    && commonWorkPool.equals(mergedWorkPool);
                        }
                        return false;
                    }, 10_000);
        }
    }

    @Test
    public void shouldRunDistributedJob() throws Exception {
        final String nodeId = "worker";
        StubbedMultiJob testJob = Mockito.spy(new StubbedMultiJob(1, getWorkItems(1)));
        try (
                CuratorFramework curator = zkTestingServer.createClient();
                DistributedJobManager jobManager = createNewJobManager(
                        nodeId,
                        curator,
                        Collections.singletonList(testJob)
                )
        ) {
            verify(testJob, timeout(10_000)).run(any());
        }
    }

    @Test
    public void shouldRunDistributedJob_whichThrowsException() throws Exception {
        final String nodeId = "worker";
        StubbedMultiJob testJob = Mockito.spy(new StubbedMultiJob(1, getWorkItems(1)));
        doThrow(new IllegalStateException("Exception in job :#)))")).when(testJob).run(any());
        try (
                CuratorFramework curator = zkTestingServer.createClient();
                DistributedJobManager jobManager = createNewJobManager(
                        nodeId,
                        curator,
                        Collections.singletonList(testJob))
        ) {
            verify(testJob, timeout(10_000).times(10)).run(any());
        }
    }


    @Test
    public void shouldRunAndRebalanceDistributedJob() throws Exception {
        final String nodeId = "worker";
        StubbedMultiJob testJob = Mockito.spy(new StubbedMultiJob(10, getWorkItems(10)));

        try (
                CuratorFramework curator = zkTestingServer.createClient();
                DistributedJobManager jobManager = createNewJobManager(
                        nodeId,
                        curator,
                        Collections.singletonList(testJob)
                )
        ) {
            RetryAssert.assertTrue(
                    () -> "Single distributed job should has all work item" + printZkTree(JOB_MANAGER_ZK_ROOT_PATH),
                    () -> testJob.getLocalWorkPool().size() == testJob.getWorkPool().getItems().size(),
                    DEFAULT_TIMEOUT);

            StubbedMultiJob testJob2 = new StubbedMultiJob(10, getWorkItems(10));
            try (
                    CuratorFramework curator2 = zkTestingServer.createClient();
                    DistributedJobManager jobManager2 = createNewJobManager(
                            "worker-2",
                            curator2,
                            Collections.singletonList(testJob2)
                    )
            ) {
                RetryAssert.assertTrue(
                        () -> "Work pool should be distributed on 2 worker" + printZkTree
                                (JOB_MANAGER_ZK_ROOT_PATH)
                                + " localPool1 " + testJob.getLocalWorkPool()
                                + " localPool2 " + testJob2.getLocalWorkPool(),
                        () -> {
                            Set<String> localPool1 = testJob.getLocalWorkPool();
                            int localPoolSize1 = localPool1 != null ? localPool1.size() : 0;

                            Set<String> localPool2 = testJob2.getLocalWorkPool();
                            int localPoolSize2 = localPool2 != null ? localPool2.size() : 0;

                            return localPoolSize1 != 0
                                    && localPoolSize2 != 0
                                    && testJob.getWorkPool().getItems().size() == localPoolSize1 + localPoolSize2;
                        },
                        30_000);
            }
        }
    }

    @Test
    public void shouldRunAndRebalanceDistributedJob_AfterHardShutdown() throws Exception {
        final String nodeId = "worker";
        StubbedMultiJob testJob = Mockito.spy(new StubbedMultiJob(10, getWorkItems(10)));

        ZkPathsManager paths = new ZkPathsManager(JOB_MANAGER_ZK_ROOT_PATH);
        // simulate hard shutdown where availability is not cleaned up
        String availableWorkpoolPath = paths.availableWorkPool(testJob.getJobId());
        zkTestingServer.getClient().create().creatingParentsIfNeeded().forPath(availableWorkpoolPath);

        try (
                CuratorFramework curator = zkTestingServer.createClient();
                DistributedJobManager jobManager = createNewJobManager(
                        nodeId,
                        curator,
                        Collections.singletonList(testJob)
                )
        ) {
            RetryAssert.assertTrue(
                    () -> "Single distributed job should has all work item" + printZkTree(JOB_MANAGER_ZK_ROOT_PATH),
                    () -> testJob.getLocalWorkPool().size() == testJob.getWorkPool().getItems().size(),
                    DEFAULT_TIMEOUT);
        }
    }

    @Test
    public void shouldMinimizeWorkerSingleThreadFactoryJobExecution() throws Exception {
        StubbedMultiJob testJob = Mockito.spy(new StubbedMultiJob(10, getWorkItems(10), Long.MAX_VALUE));
        try (
                CuratorFramework curator = zkTestingServer.createClient();
                DistributedJobManager jobManager = createNewJobManager(
                        "app-1",
                        curator,
                        Collections.singletonList(testJob)
                )
        ) {
            RetryAssert.assertTrue(
                    () -> "Single distributed job should has all work item" + printZkTree
                            (JOB_MANAGER_ZK_ROOT_PATH),
                    () -> testJob.getLocalWorkPool().size() == testJob.getWorkPool().getItems().size(),
                    DEFAULT_TIMEOUT);
            verify(testJob, timeout(500)).run(any());
        }
    }

    @Test
    public void shouldMinimizeWorkerJobExecutionAfterAnotherJobUpdate() throws Exception {
        StubbedMultiJob testJob = Mockito.spy(new StubbedMultiJob(10, getWorkItems(10), Long.MAX_VALUE));
        StubbedMultiJob testJob2 = new StubbedMultiJob(11, getWorkItems(11), Long.MAX_VALUE);

        try (
                DistributedJobManager jobManager = createNewJobManager(
                        "app-1",
                        zkTestingServer.getClient(),
                        Collections.singletonList(testJob)
                )
        ) {
            RetryAssert.assertTrue(
                    () -> "Single distributed job should has all work item" + printZkTree(JOB_MANAGER_ZK_ROOT_PATH),
                    () -> testJob.getLocalWorkPool().size() == testJob.getWorkPool().getItems().size(),
                    DEFAULT_TIMEOUT);
            verify(testJob, timeout(500)).run(any());

            try (
                    DistributedJobManager jobManager2 = createNewJobManager(
                            "app-2",
                            zkTestingServer.createClient(),
                            Collections.singletonList(testJob2)
                    )
            ) {
                RetryAssert.assertTrue(() -> "Single distributed job2 should has all work item" + printZkTree(JOB_MANAGER_ZK_ROOT_PATH),
                        () -> testJob2.getLocalWorkPool().size() == testJob2.getWorkPool().getItems().size(),
                        DEFAULT_TIMEOUT);
                verify(testJob).run(any());
            }

        }
    }

    @Test
    public void shouldMinimizeWorkerMultiThreadFactoryJobExecution() throws Exception {
        StubbedMultiJob testJob = Mockito.spy(new StubbedMultiJob(10, getWorkItems(10), 3600_000, false)); // don't pass too
        // big value here
        try (
                DistributedJobManager jobManager = createNewJobManager(
                        "app-1",
                        zkTestingServer.createClient(),
                        Collections.singletonList(testJob)
                )
        ) {
            RetryAssert.assertTrue(
                    () -> "Single distributed job should has all work item" + printZkTree
                            (JOB_MANAGER_ZK_ROOT_PATH) + testJob.getAllWorkPools(),
                    () -> testJob.getAllWorkPools().size() == 3 &&
                            testJob.getAllWorkPools().stream().flatMap(Collection::stream).collect(Collectors.toSet())
                                    .size() == 3,
                    DEFAULT_TIMEOUT);
            // 3 times, because one thread per work item
            verify(testJob, timeout(1_000).times(3)).run(any());
        }
    }

    @Test
    public void shouldUpdateWorkPool() throws Exception {
        Set<String> initialWorkPool = getWorkItems(10);
        StubbedMultiJob testJobOnWorker1 = new StubbedMultiJob(10, initialWorkPool, 100, 3000);
        StubbedMultiJob testJobOnWorker2 = new StubbedMultiJob(10, initialWorkPool, 100, 3000);

        try (
                DistributedJobManager jobManager1 = createNewJobManager(
                        "app-1",
                        zkTestingServer.createClient(),
                        Collections.singletonList(testJobOnWorker1)
                );
                DistributedJobManager jobManager2 = createNewJobManager(
                        "app-2",
                        zkTestingServer.createClient(),
                        Collections.singletonList(testJobOnWorker2)
                )

        ) {
            verifyWorkPoolIsDistributedBetweenWorkers(
                    initialWorkPool,
                    30_000,
                    testJobOnWorker1, testJobOnWorker2);

            Set<String> updatedWorkPool = new HashSet<>(Arrays.asList(
                    getWorkPool(10, 1),
                    getWorkPool(10, 4)));

            testJobOnWorker1.updateWorkPool(updatedWorkPool);
            testJobOnWorker2.updateWorkPool(updatedWorkPool);

            verifyWorkPoolIsDistributedBetweenWorkers(
                    updatedWorkPool,
                    30_000,
                    testJobOnWorker1, testJobOnWorker2);
        }
    }


    @Test
    public void rebalance_WHEN_workPool_changes_THEN_assigned_tree_changes_accordingly() throws Exception {
        Set<String> workItemsBeforeUpdating = new HashSet<>(Arrays.asList(
                getWorkPool(10, 1),
                getWorkPool(10, 2),
                getWorkPool(10, 3)));
        Set<String> workItemsAfterUpdating = new HashSet<>(Arrays.asList(
                getWorkPool(10, 2),
                getWorkPool(10, 3),
                getWorkPool(10, 4)));

        StubbedMultiJob job1 = new StubbedMultiJob(10, workItemsBeforeUpdating, 100, 10);
        StubbedMultiJob job2 = new StubbedMultiJob(10, workItemsBeforeUpdating, 100, 10);
        try (
                CuratorFramework zkClient1 = zkTestingServer.createClient();
                DistributedJobManager jobManager1 = createNewJobManager(
                        "app-1",
                        zkClient1,
                        Collections.singletonList(job1)
                );
                CuratorFramework zkClient2 = zkTestingServer.createClient();
                DistributedJobManager jobManager2 = createNewJobManager(
                        "app-2",
                        zkClient2,
                        Collections.singletonList(job2)
                )
        ) {
            verifyWorkPoolIsDistributedBetweenWorkers(workItemsBeforeUpdating, 5_000, job1, job2);

            job1.updateWorkPool(workItemsAfterUpdating);
            job2.updateWorkPool(workItemsAfterUpdating);

            verifyWorkPoolIsDistributedBetweenWorkers(workItemsAfterUpdating, 5_000, job1, job2);
        }
    }

    @Test
    public void shouldBalanceOnWorkPoolMultipleUpdate() throws Exception {
        Set<String> workPoolBeforeUpdates = getWorkItems(10);
        StubbedMultiJob testJobOnWorker1 = new StubbedMultiJob(10, workPoolBeforeUpdates, 100, 500);
        StubbedMultiJob testJobOnWorker2 = new StubbedMultiJob(10, workPoolBeforeUpdates, 100, 500);
        AggregatingProfiler profiler = new AggregatingProfiler();

        try (
                CuratorFramework curator = zkTestingServer.createClient();
                DistributedJobManager jobManager = createNewJobManager(
                        "app-1",
                        zkTestingServer.createClient(),
                        Collections.singletonList(testJobOnWorker1)
                );
                DistributedJobManager jobManager2 = createNewJobManager(
                        "app-2",
                        zkTestingServer.createClient(),
                        Collections.singletonList(testJobOnWorker2)
                )
        ) {
            verifyWorkPoolIsDistributedBetweenWorkers(
                    workPoolBeforeUpdates,
                    30_000,
                    testJobOnWorker1, testJobOnWorker2);

            List<CompletableFuture<Void>> allUpdates = new ArrayList<>();
            Set<String> updatedWorkPool1 = Set.of(
                    getWorkPool(10, 3),
                    getWorkPool(10, 4));
            Set<String> updatedWorkPool2 = Set.of(
                    getWorkPool(10, 1),
                    getWorkPool(10, 5),
                    getWorkPool(10, 6));
            for (int i = 0; i < 100; i++) {
                allUpdates.add(CompletableFuture.runAsync(() -> {
                    testJobOnWorker1.updateWorkPool(updatedWorkPool1);
                    testJobOnWorker2.updateWorkPool(updatedWorkPool1);
                }));
                allUpdates.add(CompletableFuture.runAsync(() -> {
                    testJobOnWorker1.updateWorkPool(updatedWorkPool2);
                    testJobOnWorker2.updateWorkPool(updatedWorkPool2);
                }));
            }
            CompletableFuture.allOf(allUpdates.toArray(new CompletableFuture[0])).join();

            testJobOnWorker1.updateWorkPool(updatedWorkPool2);
            testJobOnWorker2.updateWorkPool(updatedWorkPool2);

            verifyWorkPoolIsDistributedBetweenWorkers(
                    updatedWorkPool2,
                    50_000,
                    testJobOnWorker1, testJobOnWorker2);
        }
    }

    //    @Test
    public void shouldAddAndRemoveDistributedJob() throws Exception {
        final String[] nodeIds = {"added-worker-1", "added-worker-2"};
        CuratorFramework curator1 = zkTestingServer.createClient();
        DistributedJobManager jobManager1 = createNewJobManager(nodeIds[0], curator1);
        try (
                CuratorFramework curator2 = zkTestingServer.createClient();
                DistributedJobManager jobManager2 = createNewJobManager(nodeIds[1], curator2)
        ) {
            jobManager1.close();
            curator1.close();

            RetryAssert.assertTrue(
                    () -> "All work pool should be distributed on 1 alive worker" + printZkTree
                            (JOB_MANAGER_ZK_ROOT_PATH),
                    () -> {
                        List<String> workPoolForFirstJob = curator2.getChildren()
                                .forPath(paths.assignedWorkPool(nodeIds[1], getJobId(1)));
                        return workPoolForFirstJob.size() == getWorkItems(1).size();
                    }, 30_000);
        }
    }


    private void verifyWorkPoolIsDistributedBetweenWorkers(
            Set<String> commonWorkPool, long durationMs, StubbedMultiJob... jobsOnWorkers) throws Exception {

        RetryAssert.assertTrue(
                () -> "Work pools distributed between two workers " + commonWorkPool
                        + " localPools: " + Arrays.stream(jobsOnWorkers)
                        .map(StubbedMultiJob::getLocalWorkPool)
                        .collect(Collectors.toUnmodifiableList())
                        + printZkTree(JOB_MANAGER_ZK_ROOT_PATH),
                () -> {
                    List<Set<String>> localWorkPools = Arrays.stream(jobsOnWorkers)
                            .map(StubbedMultiJob::getLocalWorkPool)
                            .collect(Collectors.toUnmodifiableList());

                    Set<String> commonWorkPoolFromLocals = localWorkPools.stream()
                            .flatMap(Collection::stream)
                            .collect(Collectors.toUnmodifiableSet());
                    return commonWorkPoolFromLocals.equals(commonWorkPool)
                            && setsSizesDifferLessThanTwo(localWorkPools);
                }, durationMs);
    }

    private boolean setsSizesDifferLessThanTwo(List<Set<String>> sets) {
        Set<String> firstSet = sets.get(0);
        int maxSize = firstSet.size();
        int minSize = maxSize;
        for (Set<String> set : sets) {
            int size = set.size();
            maxSize = Integer.max(maxSize, size);
            minSize = Integer.min(minSize, size);
            if (maxSize - minSize > 2) {
                return false;
            }
        }
        return true;
    }


    private DistributedJobManager createNewJobManager(
            String nodeId,
            CuratorFramework curatorFramework
    ) throws Exception {
        return createNewJobManager(
                nodeId,
                curatorFramework,
                Arrays.asList(
                        new StubbedMultiJob(1, getWorkItems(1)),
                        new StubbedMultiJob(2, getWorkItems(2)),
                        new StubbedMultiJob(3, getWorkItems(3)))
        );
    }

    private DistributedJobManager createNewJobManager(
            String nodeId,
            CuratorFramework curatorFramework,
            Collection<DistributedJob> collection
    ) throws Exception {
        return new DistributedJobManager(
                curatorFramework,
                collection,
                new AggregatingProfiler(),
                new DistributedJobManagerSettings(
                        nodeId,
                        JOB_MANAGER_ZK_ROOT_PATH,
                        AssignmentStrategies.Companion.getDEFAULT(),
                        getTerminationWaitTime()
                )
        );
    }

    private DynamicProperty<Long> getTerminationWaitTime() {
        return DynamicProperty.of(180_000L);

    }

    private Set<String> getWorkItems(int jobId) {
        return new HashSet<>(Arrays.asList(
                getWorkPool(jobId, 1),
                getWorkPool(jobId, 2),
                getWorkPool(jobId, 3)));
    }

    private String getWorkPool(int jobId, int workItemIndex) {
        return String.format("work-item-%d.%d", jobId, workItemIndex);
    }

}
