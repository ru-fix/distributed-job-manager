package ru.fix.distributed.job.manager;

import org.apache.curator.framework.CuratorFramework;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.aggregating.profiler.AggregatingProfiler;
import ru.fix.distributed.job.manager.model.DistributedJobManagerSettings;
import ru.fix.distributed.job.manager.strategy.AssignmentStrategies;
import ru.fix.dynamic.property.api.DynamicProperty;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;
import static ru.fix.distributed.job.manager.StubbedMultiJob.getJobId;

/**
 * @author Ayrat Zulkarnyaev
 */
public class WorkPooledMultiJobIT extends AbstractJobManagerTest {
    private static final int DEFAULT_TIMEOUT_SEC = 15;

    private static final Logger logger = LoggerFactory.getLogger(WorkPooledMultiJobIT.class);

    @Test
    public void shouldAddNewAvailableWorkPool() throws Exception {
        final String nodeId = "common-worker-1";
        try (
                CuratorFramework curator = zkTestingServer.createClient();
                DistributedJobManager jobManager1 = createNewJobManager(nodeId, curator)
        ) {
            await().atMost(30, TimeUnit.SECONDS).untilAsserted(() ->
                    assertNodeExists(paths.availableWorkItem(getJobId(1), "work-item-1.1"), curator)
            );
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
            await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
                String assertionMessage = "ZK checks failure. " + printZkTree(JOB_MANAGER_ZK_ROOT_PATH);
                List<String> totalWorkPool = new ArrayList<>(3);
                // Work pool contains 3 work items. Then every distributed job should contains 1 work item.
                for (String nodeId : nodeIds) {
                    String assignedWorkPoolPath = paths.assignedWorkPool(nodeId, getJobId(1));
                    assertNodeExists(assignedWorkPoolPath, curator);
                    List<String> workPool = curator.getChildren().forPath(assignedWorkPoolPath);
                    assertThat(assertionMessage, workPool.size(), equalTo(1));
                    totalWorkPool.addAll(workPool);
                }
                assertThat(assertionMessage, totalWorkPool.contains(searchedWorkItem));
            });
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
            await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {

                String pathForWorker1 = paths.assignedWorkPool(nodeIds[0], getJobId(1));
                String pathForWorker2 = paths.assignedWorkPool(nodeIds[1], getJobId(1));

                assertNodeExists(pathForWorker1, curator);
                assertNodeExists(pathForWorker2, curator);

                List<String> firstWorkPool =
                        curator.getChildren().forPath(pathForWorker1);

                List<String> secondWorkPool =
                        curator.getChildren().forPath(pathForWorker2);

                Set<String> mergedWorkPool = new HashSet<>();
                mergedWorkPool.addAll(firstWorkPool);
                mergedWorkPool.addAll(secondWorkPool);

                String assertionMessage = "ZK checks failure. " + printZkTree(JOB_MANAGER_ZK_ROOT_PATH);
                assertThat(assertionMessage, !firstWorkPool.isEmpty());
                assertThat(assertionMessage, !secondWorkPool.isEmpty());
                assertThat(assertionMessage, mergedWorkPool, equalTo(getWorkItems(1)));
            });
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
            verifySingleJobIsDistributedBetweenWorkers(DEFAULT_TIMEOUT_SEC, testJob);

            StubbedMultiJob testJob2 = new StubbedMultiJob(10, getWorkItems(10));
            try (
                    CuratorFramework curator2 = zkTestingServer.createClient();
                    DistributedJobManager jobManager2 = createNewJobManager(
                            "worker-2",
                            curator2,
                            Collections.singletonList(testJob2)
                    )
            ) {
                verifySingleJobIsDistributedBetweenWorkers(30_000, testJob, testJob2);
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
            verifySingleJobIsDistributedBetweenWorkers(DEFAULT_TIMEOUT_SEC, testJob);
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
            verifySingleJobIsDistributedBetweenWorkers(DEFAULT_TIMEOUT_SEC, testJob);
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
            verifySingleJobIsDistributedBetweenWorkers(DEFAULT_TIMEOUT_SEC, testJob);
            verify(testJob, timeout(500)).run(any());

            try (
                    DistributedJobManager jobManager2 = createNewJobManager(
                            "app-2",
                            zkTestingServer.createClient(),
                            Collections.singletonList(testJob2)
                    )
            ) {
                verifySingleJobIsDistributedBetweenWorkers(DEFAULT_TIMEOUT_SEC, testJob2);
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
            await().atMost(DEFAULT_TIMEOUT_SEC, TimeUnit.SECONDS).untilAsserted(() -> {
                Set<String> workPoolFromAllThreads = testJob.getAllWorkPools()
                        .stream()
                        .flatMap(Collection::stream)
                        .collect(Collectors.toSet());

                assertThat("Single distributed job should has all work item" + printZkTree(JOB_MANAGER_ZK_ROOT_PATH),
                        workPoolFromAllThreads, equalTo(testJob.getWorkPool().getItems()));
            });
            // 3 times, because one thread per work item
            verify(testJob, timeout(1_000).times(3)).run(any());
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
            verifySingleJobIsDistributedBetweenWorkers(
                    30_000,
                    testJobOnWorker1, testJobOnWorker2);

            Set<String> updatedWorkPool = new HashSet<>(Arrays.asList(
                    getWorkPool(10, 1),
                    getWorkPool(10, 4)));

            testJobOnWorker1.updateWorkPool(updatedWorkPool);
            testJobOnWorker2.updateWorkPool(updatedWorkPool);

            verifySingleJobIsDistributedBetweenWorkers(
                    30_000,
                    testJobOnWorker1, testJobOnWorker2);
        }
    }

    @Test
    public void shouldBalanceOnWorkPoolMultipleUpdate() throws Exception {
        StubbedMultiJob testJobOnWorker1 = new StubbedMultiJob(10, getWorkItems(10), 100, 500);
        StubbedMultiJob testJobOnWorker2 = new StubbedMultiJob(10, getWorkItems(10), 100, 500);

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
            verifySingleJobIsDistributedBetweenWorkers(
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

            verifySingleJobIsDistributedBetweenWorkers(
                    50_000,
                    testJobOnWorker1, testJobOnWorker2);
        }
    }

    @Test
    public void cleaning_WHEN_last_djm_with_available_job_closed_THEN_removing_job_from_workPool() throws Exception {
        DistributedJob job1 = new StubbedMultiJob(1, getWorkItems(4));
        DistributedJob job2 = new StubbedMultiJob(2, getWorkItems(4));
        DistributedJob job3 = new StubbedMultiJob(3, getWorkItems(4));
        long awaitCleaningTimeout = getWorkPoolCleanPeriod().get() * 3;

        CuratorFramework curator1 = zkTestingServer.createClient();
        DistributedJobManager jobManager1 = createNewJobManager(
                "djm-1",
                curator1,
                List.of(job1, job2)
        );
        try (
                CuratorFramework curator2 = zkTestingServer.createClient();
                DistributedJobManager jobManager2 = createNewJobManager(
                        "djm-2",
                        curator2,
                        List.of(job2, job3)
                )
        ) {
            assertTrue(curator2.getChildren().forPath(paths.availableWorkPool()).contains(job1.getJobId()));

            jobManager1.close();
            curator1.close();

            assertTimeoutPreemptively(Duration.ofMillis(awaitCleaningTimeout), () -> {
                        while (curator2.getChildren().forPath(paths.availableWorkPool()).contains(job1.getJobId())) {
                            Thread.sleep(getWorkPoolCleanPeriod().get());
                        }
                    },
                    "cleaning wasn't performed in " + awaitCleaningTimeout
            );
        }
    }


    //    @Test
    public void shouldAddAndRemoveDistributedJob() throws Exception {
        final String[] nodeIds = {"added-worker-1", "added-worker-2"};

        CuratorFramework curator1 = zkTestingServer.createClient();
        DistributedJobManager jobManager1 = createNewJobManager(nodeIds[0], curator1);
        CuratorFramework curator2 = zkTestingServer.createClient();
        DistributedJobManager jobManager2 = createNewJobManager(nodeIds[1], curator2);

        jobManager1.close();
        curator1.close();

        Set<String> totalWorkPoolForFirstJob = getWorkItems(1);
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {

            List<String> workPoolForFirstJobOnSecondWorker = curator2.getChildren()
                    .forPath(paths.assignedWorkPool(nodeIds[1], getJobId(1)));

            assertThat(String.format("the only alive worker should have all work-pool of job, but it has %s instead of %s",
                    workPoolForFirstJobOnSecondWorker, totalWorkPoolForFirstJob) + printZkTree(JOB_MANAGER_ZK_ROOT_PATH),
                    collectionsAreEqual(totalWorkPoolForFirstJob, workPoolForFirstJobOnSecondWorker)
            );
        });

        jobManager2.close();
        curator2.close();
    }

    private void assertNodeExists(String zkPath, CuratorFramework client) throws Exception {
        assertThat(
                String.format("Node %s is not exists. ", zkPath) + printZkTree(JOB_MANAGER_ZK_ROOT_PATH),
                client.checkExists().forPath(zkPath), notNullValue());
    }

    /**
     * Checks: <br>
     * local workPools of jobs in total give common work-pool <br>
     * local workPools of jobs sizes differs less than two
     *
     * @param jobInstancesOnWorkers instances of single job from every worker in cluster,
     *                              which can proceed *only* that job.
     */
    private void verifySingleJobIsDistributedBetweenWorkers(
            long durationSec, StubbedMultiJob... jobInstancesOnWorkers) {

        if (!jobsHasSameIdAndSameWorkPool(jobInstancesOnWorkers)) {
            throw new IllegalArgumentException("This method can verify workPool distribution only if workers have same single job." +
                    "given stubbed jobs: " + Arrays.toString(jobInstancesOnWorkers));
        }

        Set<String> commonWorkPool = jobInstancesOnWorkers[0].getWorkPool().getItems();

        await().atMost(durationSec, TimeUnit.SECONDS).untilAsserted(() -> {
            List<Set<String>> localWorkPools = Arrays.stream(jobInstancesOnWorkers)
                    .map(StubbedMultiJob::getLocalWorkPool)
                    .collect(Collectors.toUnmodifiableList());

            List<String> commonWorkPoolFromLocals = localWorkPools.stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());

            assertThat(
                    String.format("work-pool from locals isn't equal common work-pool. localWorkPools=%s commonWorkPool=%s"
                            + printZkTree(JOB_MANAGER_ZK_ROOT_PATH), commonWorkPool, commonWorkPoolFromLocals, localWorkPools),
                    collectionsAreEqual(commonWorkPoolFromLocals, commonWorkPool)
            );
            assertThat(
                    String.format("work-pool isn't distributed evenly. localWorkPools={%s}"
                            + printZkTree(JOB_MANAGER_ZK_ROOT_PATH), localWorkPools),
                    setsSizesDifferLessThanTwo(localWorkPools)
            );
        });
    }

    private boolean collectionsAreEqual(Collection<String> c1, Collection<String> c2) {
        return c1.size() == c2.size() && c1.containsAll(c2);
    }

    private boolean jobsHasSameIdAndSameWorkPool(DistributedJob... jobs) {
        DistributedJob firstJob = jobs[0];
        String id = firstJob.getJobId();
        Set<String> workPool = firstJob.getWorkPool().getItems();
        for (DistributedJob nextJob : jobs) {
            String nextId = nextJob.getJobId();
            Set<String> nextWorkPool = nextJob.getWorkPool().getItems();
            if (!nextId.equals(id) || !nextWorkPool.equals(workPool)) {
                return false;
            }
        }
        return true;
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
                        getTerminationWaitTime(),
                        getWorkPoolCleanPeriod()
                )
        );
    }

    private DynamicProperty<Long> getWorkPoolCleanPeriod() {
        return DynamicProperty.of(1_000L);
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
