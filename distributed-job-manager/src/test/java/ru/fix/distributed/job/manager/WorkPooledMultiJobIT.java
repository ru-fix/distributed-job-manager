package ru.fix.distributed.job.manager;

import org.apache.curator.framework.CuratorFramework;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.fix.distributed.job.manager.model.JobDescriptor;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;
import static ru.fix.distributed.job.manager.StubbedMultiJob.getJobId;
import static ru.fix.distributed.job.manager.StubbedMultiJobKt.awaitSingleJobIsDistributedBetweenWorkers;

/**
 * @author Ayrat Zulkarnyaev
 */
public class WorkPooledMultiJobIT extends AbstractJobManagerTest {
    private static final int DEFAULT_TIMEOUT_SEC = 15;

    @Test
    public void shouldRunAndRebalanceDistributedJob() throws Exception {
        StubbedMultiJob testJob = new StubbedMultiJob(10, getWorkItems(10));

        try (
                CuratorFramework curator = defaultZkClient();
                DistributedJobManager jobManager = createNewJobManager(Collections.singletonList(testJob), curator)
        ) {
            awaitSingleJobIsDistributedBetweenWorkers(DEFAULT_TIMEOUT_SEC, testJob);

            StubbedMultiJob testJob2 = new StubbedMultiJob(10, getWorkItems(10));
            try (
                    CuratorFramework curator2 = defaultZkClient();
                    DistributedJobManager jobManager2 = createNewJobManager(
                            Collections.singletonList(testJob2),
                            curator2
                    )
            ) {
                awaitSingleJobIsDistributedBetweenWorkers(30, testJob, testJob2);
            }
        }
    }

    @Test
    public void shouldRunAndRebalanceDistributedJob_AfterHardShutdown() throws Exception {
        StubbedMultiJob testJob = new StubbedMultiJob(10, getWorkItems(10));

        ZkPathsManager paths = new ZkPathsManager(JOB_MANAGER_ZK_ROOT_PATH);
        // simulate hard shutdown where availability is not cleaned up
        String availableWorkpoolPath = paths.availableWorkPool(new JobDescriptor(testJob).getJobId().getId());
        zkTestingServer.getClient().create().creatingParentsIfNeeded().forPath(availableWorkpoolPath);

        try (
                CuratorFramework curator = defaultZkClient();
                DistributedJobManager jobManager = createNewJobManager(Collections.singletonList(testJob), curator)
        ) {
            awaitSingleJobIsDistributedBetweenWorkers(DEFAULT_TIMEOUT_SEC, testJob);
        }
    }

    @Test
    public void shouldMinimizeWorkerSingleThreadFactoryJobExecution() throws Exception {
        StubbedMultiJob testJob = Mockito.spy(new StubbedMultiJob(10, getWorkItems(10), Long.MAX_VALUE));
        try (
                CuratorFramework curator = defaultZkClient();
                DistributedJobManager jobManager = createNewJobManager(Collections.singletonList(testJob), curator)
        ) {
            awaitSingleJobIsDistributedBetweenWorkers(DEFAULT_TIMEOUT_SEC, testJob);
            verify(testJob, timeout(500)).run(any());
        }
    }

    @Test
    public void shouldMinimizeWorkerJobExecutionAfterAnotherJobUpdate() throws Exception {
        StubbedMultiJob testJob = Mockito.spy(new StubbedMultiJob(10, getWorkItems(10), Long.MAX_VALUE));
        StubbedMultiJob testJob2 = new StubbedMultiJob(11, getWorkItems(11), Long.MAX_VALUE);

        try (
                DistributedJobManager jobManager = createNewJobManager(Collections.singletonList(testJob))
        ) {
            awaitSingleJobIsDistributedBetweenWorkers(DEFAULT_TIMEOUT_SEC, testJob);
            verify(testJob, timeout(500)).run(any());

            try (
                    DistributedJobManager jobManager2 = createNewJobManager(Collections.singletonList(testJob2))
            ) {
                awaitSingleJobIsDistributedBetweenWorkers(DEFAULT_TIMEOUT_SEC, testJob2);
                verify(testJob).run(any());
            }

        }
    }



    @Test
    public void shouldUpdateWorkPool() throws Exception {
        StubbedMultiJob testJobOnWorker1 = new StubbedMultiJob(10, getWorkItems(10), 100, 3000);
        StubbedMultiJob testJobOnWorker2 = new StubbedMultiJob(10, getWorkItems(10), 100, 3000);

        try (
                DistributedJobManager jobManager1 = createNewJobManager(Collections.singletonList(testJobOnWorker1));
                DistributedJobManager jobManager2 = createNewJobManager(Collections.singletonList(testJobOnWorker2))
        ) {
            awaitSingleJobIsDistributedBetweenWorkers(30, testJobOnWorker1, testJobOnWorker2);

            Set<String> updatedWorkPool = new HashSet<>(Arrays.asList(
                    getWorkPool(10, 1),
                    getWorkPool(10, 4)));

            testJobOnWorker1.updateWorkPool(updatedWorkPool);
            testJobOnWorker2.updateWorkPool(updatedWorkPool);

            awaitSingleJobIsDistributedBetweenWorkers(30, testJobOnWorker1, testJobOnWorker2);
        }
    }

    @Test
    public void shouldBalanceOnWorkPoolMultipleUpdate() throws Exception {
        StubbedMultiJob testJobOnWorker1 = new StubbedMultiJob(10, getWorkItems(10), 100, 500);
        StubbedMultiJob testJobOnWorker2 = new StubbedMultiJob(10, getWorkItems(10), 100, 500);

        try (
                DistributedJobManager jobManager = createNewJobManager(Collections.singletonList(testJobOnWorker1));
                DistributedJobManager jobManager2 = createNewJobManager(Collections.singletonList(testJobOnWorker2))
        ) {
            awaitSingleJobIsDistributedBetweenWorkers(30, testJobOnWorker1, testJobOnWorker2);

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

            awaitSingleJobIsDistributedBetweenWorkers(50, testJobOnWorker1, testJobOnWorker2);
        }
    }

    //    @Test
    public void shouldAddAndRemoveDistributedJob() throws Exception {
        final String[] nodeIds = {"added-worker-1", "added-worker-2"};

        CuratorFramework curator1 = defaultZkClient();
        DistributedJobManager jobManager1 = createNewJobManager(nodeIds[0], curator1);
        CuratorFramework curator2 = defaultZkClient();
        DistributedJobManager jobManager2 = createNewJobManager(nodeIds[1], curator2);

        jobManager1.close();
        curator1.close();

        Set<String> totalWorkPoolForFirstJob = getWorkItems(1);
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {

            List<String> workPoolForFirstJobOnSecondWorker = curator2.getChildren()
                    .forPath(paths.assignedWorkPool(nodeIds[1], getJobId(1).getId()));

            assertThat(String.format("the only alive worker should have all work-pool of job, but it has %s instead of %s",
                    workPoolForFirstJobOnSecondWorker, totalWorkPoolForFirstJob) + printDjmZkTree(),
                    workPoolForFirstJobOnSecondWorker.size() == totalWorkPoolForFirstJob.size()
                            && workPoolForFirstJobOnSecondWorker.containsAll(totalWorkPoolForFirstJob)
            );
        });

        jobManager2.close();
        curator2.close();
    }

    private DistributedJobManager createNewJobManager(
            String nodeId,
            CuratorFramework curatorFramework
    ) {
        return createNewJobManager(
                Arrays.asList(
                        new StubbedMultiJob(1, getWorkItems(1)),
                        new StubbedMultiJob(2, getWorkItems(2)),
                        new StubbedMultiJob(3, getWorkItems(3))),
                curatorFramework,
                nodeId
        );
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
