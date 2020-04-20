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
import ru.fix.distributed.job.manager.model.DistributedJobManagerSettings;
import ru.fix.distributed.job.manager.model.DistributedJobSettings;
import ru.fix.distributed.job.manager.strategy.AssignmentStrategies;

import ru.fix.dynamic.property.api.DynamicProperty;
import ru.fix.stdlib.socket.proxy.ProxySocket;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.mockito.Mockito.*;
import static ru.fix.distributed.job.manager.DistributedJobManagerConfigHelper.*;
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

            assertTimeout(
                    Duration.ofMillis(30_000),
                    () -> {
                        String jobId = getJobId(1);
                        Stat commonWorkerPoolChecker = zkTestingServer.getClient().checkExists()
                                .forPath(paths.availableWorkItem(jobId, "work-item-1.1"));
                        return commonWorkerPoolChecker != null;
                    },
                    () -> "Wait for assignment common-worker-1 --> work-item-1.1" + printZkTree
                            (JOB_MANAGER_ZK_ROOT_PATH));
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
            assertTimeout(Duration.ofMillis(30_000),
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
                    }, "Wait for assignment work-item-1.1 to any worker");
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
            assertTimeout(Duration.ofMillis(30_000),
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
                    },
                    "All jobs assigned between workers");

            proxySocket.close();
            logger.info("Closed proxy to emulate network issue. Curator connection timeout is {}. Waiting {} + 10_000" +
                    " ms until connection lost.", sessionTimeout, 3_000);
            assertTimeout(Duration.ofMillis(sessionTimeout + 3_000),
                    () -> !proxiedCurator.getZookeeperClient().getZooKeeper().getState().isConnected(),
                    "Curator is disconnected");

            assertTimeout(Duration.ofMillis(30_000),
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
                    }, "All jobs assigned to second worker");

            logger.info("Restored proxy on the same port {}. Curator could reconnect now.", proxySocket.getPort());

            assertTimeout(
                    Duration.ofMillis(DEFAULT_TIMEOUT),
                    () -> {
                        try {
                            return proxiedCurator.getZookeeperClient().getZooKeeper().getState().isConnected();
                        } catch (Exception e) {
                            return false;
                        }
                    }, "Curator is connected");

            assertTimeout(Duration.ofMillis(50_000),
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
                    },
                    () -> "Jobs was again distributed between workers (and worker1 received at least " +
                            "one pool back) " + printZkTree(JOB_MANAGER_ZK_ROOT_PATH));
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
            assertTimeout(Duration.ofMillis(10_000),
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
                    },
                    () -> "All work pool should distribute to workers " + printZkTree
                            (JOB_MANAGER_ZK_ROOT_PATH));
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
            assertTimeout(Duration.ofMillis(10_000),
                    () -> Mockito.mockingDetails(testJob).getInvocations()
                            .stream().anyMatch(i -> i.getMethod().getName().equals("run")),
                    () -> "Stubbed multi job completed");
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
            assertTimeout(Duration.ofMillis(10_000),
                    () -> Mockito.mockingDetails(testJob).getInvocations()
                            .stream().filter(i -> i.getMethod().getName().equals("run"))
                            .count() == 10L,
                    "Stubbed multi job with exception was run 10 times");
        }
    }


    @Test
    public void shouldRunThenStopDistributedJob() throws Exception {
        final String nodeId = "worker";
        StubbedMultiJob testJob = Mockito.spy(new StubbedMultiJob(1, getWorkItems(1)));
        List collection = Collections.singletonList(testJob);
        try (
                CuratorFramework curator = zkTestingServer.createClient();
                DistributedJobManager jobManager = createNewJobManager(
                        nodeId,
                        curator,
                        collection
                )
        ) {
            assertTimeout(Duration.ofMillis(10_000),
                    () -> Mockito.mockingDetails(testJob).getInvocations()
                            .stream().anyMatch(i -> i.getMethod().getName().equals("run")),
                    () -> "Stubbed multi job completed");
            allJobsDisabled(collection);
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
            assertTimeout(
                    Duration.ofMillis(DEFAULT_TIMEOUT),
                    () -> testJob.getLocalWorkPool().size() == testJob.getWorkPool().getItems().size(),
                    () -> "Single distributed job should has all work item" + printZkTree(JOB_MANAGER_ZK_ROOT_PATH));

            StubbedMultiJob testJob2 = new StubbedMultiJob(10, getWorkItems(10));
            try (
                    CuratorFramework curator2 = zkTestingServer.createClient();
                    DistributedJobManager jobManager2 = createNewJobManager(
                            "worker-2",
                            curator2,
                            Collections.singletonList(testJob2)
                    )
            ) {
                assertTimeout(
                        Duration.ofMillis(30_000),
                        () -> {
                            Set<String> localPool1 = testJob.getLocalWorkPool();
                            int localPoolSize1 = localPool1 != null ? localPool1.size() : 0;

                            Set<String> localPool2 = testJob2.getLocalWorkPool();
                            int localPoolSize2 = localPool2 != null ? localPool2.size() : 0;

                            return localPoolSize1 != 0
                                    && localPoolSize2 != 0
                                    && testJob.getWorkPool().getItems().size() == localPoolSize1 + localPoolSize2;
                        },
                        () -> "Work pool should be distributed on 2 worker" + printZkTree
                                (JOB_MANAGER_ZK_ROOT_PATH)
                                + " localPool1 " + testJob.getLocalWorkPool()
                                + " localPool2 " + testJob2.getLocalWorkPool());
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
            assertTimeout(
                    Duration.ofMillis(DEFAULT_TIMEOUT),
                    () -> testJob.getLocalWorkPool().size() == testJob.getWorkPool().getItems().size(),
                    () -> "Single distributed job should has all work item" + printZkTree(JOB_MANAGER_ZK_ROOT_PATH));
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
            assertTimeout(
                    Duration.ofMillis(DEFAULT_TIMEOUT),
                    () -> testJob.getLocalWorkPool().size() == testJob.getWorkPool().getItems().size(),
                    () -> "Single distributed job should has all work item" + printZkTree
                            (JOB_MANAGER_ZK_ROOT_PATH));
            Thread.sleep(500);

            verify(testJob, times(1)).run(any());
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
            assertTimeout(
                    Duration.ofMillis(DEFAULT_TIMEOUT),
                    () -> testJob.getLocalWorkPool().size() == testJob.getWorkPool().getItems().size(),
                    () -> "Single distributed job should has all work item" + printZkTree(JOB_MANAGER_ZK_ROOT_PATH));
            Thread.sleep(500);
            verify(testJob, times(1)).run(any());

            try (
                    DistributedJobManager jobManager2 = createNewJobManager(
                            "app-2",
                            zkTestingServer.createClient(),
                            Collections.singletonList(testJob)
                    )
            ) {
                assertTimeout(Duration.ofMillis(DEFAULT_TIMEOUT),
                        () -> testJob2.getLocalWorkPool().size() == testJob2.getWorkPool().getItems().size(),
                        () -> "Single distributed job2 should has all work item" + printZkTree(JOB_MANAGER_ZK_ROOT_PATH));
                verify(testJob, times(1)).run(any());
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
            assertTimeout(Duration.ofMillis(DEFAULT_TIMEOUT),
                    () -> testJob.getAllWorkPools().size() == 3 &&
                            testJob.getAllWorkPools().stream().flatMap(Collection::stream).collect(Collectors.toSet())
                                    .size() == 3,
                    () -> "Single distributed job should has all work item" + printZkTree
                            (JOB_MANAGER_ZK_ROOT_PATH) + testJob.getAllWorkPools());
            Thread.sleep(1000);

            // 3 times, because one thread per work item
            verify(testJob, times(3)).run(any());
        }
    }

    @Test
    public void shouldUpdateWorkPool() throws Exception {
        StubbedMultiJob testJobOnWorker1 = new StubbedMultiJob(10, getWorkItems(10), 100, 3000);
        StubbedMultiJob testJobOnWorker2 = new StubbedMultiJob(10, getWorkItems(10), 100, 3000);

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
            assertTimeout(Duration.ofMillis(30_000),
                    () -> {
                        int localPoolSize1 = testJobOnWorker1.getLocalWorkPool().size();
                        int localPoolSize2 = testJobOnWorker2.getLocalWorkPool().size();
                        return localPoolSize1 != 0 && localPoolSize2 != 0
                                && 3 == localPoolSize1 + localPoolSize2;
                    },
                    () -> "Work pools distributed between two workers" + printZkTree
                            (JOB_MANAGER_ZK_ROOT_PATH)
                            + " localPool1 " + testJobOnWorker1.getLocalWorkPool()
                            + " localPool2 " + testJobOnWorker2.getLocalWorkPool());


            testJobOnWorker1.updateWorkPool(new HashSet<>(Arrays.asList(
                    getWorkPool(10, 1),
                    getWorkPool(10, 4))));

            assertTimeout(
                    Duration.ofMillis(30_000),
                    () -> {
                        Set<String> worker1WorkPool = testJobOnWorker1.getLocalWorkPool();
                        int localPoolSize1 = worker1WorkPool.size();
                        int localPoolSize2 = testJobOnWorker2.getLocalWorkPool().size();
                        return localPoolSize1 != 0 && localPoolSize2 != 0
                                && 4 == localPoolSize1 + localPoolSize2
                                && worker1WorkPool.contains(getWorkPool(10, 4));
                    },
                    () -> "Work pools distributed between two workers and worker 1 has item " +
                            getWorkPool(10, 4)
                            + printZkTree(JOB_MANAGER_ZK_ROOT_PATH)
                            + " localPool1 " + testJobOnWorker1.getLocalWorkPool()
                            + " localPool2 " + testJobOnWorker2.getLocalWorkPool());
        }
    }

    @Test
    public void shouldBalanceOnWorkPoolMultipleUpdate() throws Exception {
        StubbedMultiJob testJobOnWorker1 = new StubbedMultiJob(10, getWorkItems(10), 100, 500);
        StubbedMultiJob testJobOnWorker2 = new StubbedMultiJob(10, getWorkItems(10), 100, 500);
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
            assertTimeout(Duration.ofMillis(30_000),
                    () -> {
                        int localPoolSize1 = testJobOnWorker1.getLocalWorkPool().size();
                        int localPoolSize2 = testJobOnWorker2.getLocalWorkPool().size();
                        return localPoolSize1 != 0 && localPoolSize2 != 0
                                && 3 == localPoolSize1 + localPoolSize2;
                    },
                    () -> "Work pools distributed between two workers" + printZkTree
                            (JOB_MANAGER_ZK_ROOT_PATH)
                            + " localPool1 " + testJobOnWorker1.getLocalWorkPool()
                            + " localPool2 " + testJobOnWorker2.getLocalWorkPool());


            List<CompletableFuture<Void>> allUpdates = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                allUpdates.add(CompletableFuture.runAsync(() -> testJobOnWorker1.updateWorkPool(
                        Collections.singleton(getWorkPool(10, 1)))));
            }
            CompletableFuture.allOf(allUpdates.toArray(new CompletableFuture[0])).join();

            testJobOnWorker1.updateWorkPool(new HashSet<>(Collections.singletonList(
                    getWorkPool(10, 2))));

            assertTimeout(Duration.ofMillis(50_000),
                    () -> {
                        Set<String> worker1WorkPool = testJobOnWorker1.getLocalWorkPool();
                        int localPoolSize1 = worker1WorkPool.size();
                        int localPoolSize2 = testJobOnWorker2.getLocalWorkPool().size();
                        return localPoolSize1 != 0 && localPoolSize2 != 0
                                && 3 == localPoolSize1 + localPoolSize2
                                && worker1WorkPool.contains(getWorkPool(10, 2));
                    },
                    () -> "Work pools distributed between two workers and worker 1 has item " +
                            getWorkPool(10, 2)
                            + printZkTree(JOB_MANAGER_ZK_ROOT_PATH)
                            + " localPool1 " + testJobOnWorker1.getLocalWorkPool()
                            + " localPool2 " + testJobOnWorker2.getLocalWorkPool());
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

            assertTimeout(Duration.ofMillis(30_000),
                    () -> {
                        List<String> workPoolForFirstJob = curator2.getChildren()
                                .forPath(paths.assignedWorkPool(nodeIds[1], getJobId(1)));
                        return workPoolForFirstJob.size() == getWorkItems(1).size();
                    },
                    () -> "All work pool should be distributed on 1 alive worker" + printZkTree
                            (JOB_MANAGER_ZK_ROOT_PATH));
        }
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
        Map<String,Boolean> jobsPresetSettings = allJobsEnabled(collection).get().getJobsEnabledStatus();

        return new DistributedJobManager(
                curatorFramework,
                collection,
                new AggregatingProfiler(),
                new DistributedJobManagerSettings(
                        nodeId,
                        JOB_MANAGER_ZK_ROOT_PATH,
                        AssignmentStrategies.Companion.getDEFAULT(),
                        DynamicProperty.of(new DistributedJobSettings(getTerminationWaitTime(),jobsPresetSettings))
                )
        );
    }

    private Long getTerminationWaitTime() {
        return 180_000L;

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
