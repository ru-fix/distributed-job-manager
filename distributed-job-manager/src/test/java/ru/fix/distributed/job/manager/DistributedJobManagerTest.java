package ru.fix.distributed.job.manager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.fix.aggregating.profiler.AggregatingProfiler;
import ru.fix.distributed.job.manager.model.JobId;
import ru.fix.distributed.job.manager.model.WorkItem;
import ru.fix.distributed.job.manager.model.ZookeeperState;
import ru.fix.distributed.job.manager.strategy.AssignmentStrategy;
import ru.fix.distributed.job.manager.strategy.AssignmentStrategyFactory;
import ru.fix.dynamic.property.api.DynamicProperty;
import ru.fix.stdlib.concurrency.threads.Schedule;
import ru.fix.zookeeper.testing.ZKTestingServer;

import java.util.*;

class DistributedJobManagerTest {
    private ZKTestingServer zkTestingServer;

    @BeforeEach
    public void setUp() throws Exception {
        zkTestingServer = new ZKTestingServer();
        zkTestingServer.start();
    }

    public static class DistributedJobStub implements DistributedJob {
        private final Long delay;
        private final String jobId;
        private final WorkPool workPool;

        public DistributedJobStub(String jobId, WorkPool workPool) {
            this(jobId, workPool, 1000L);
        }

        public DistributedJobStub(String jobId, WorkPool workPool, Long delay) {
            this.jobId = jobId;
            this.workPool = workPool;
            this.delay = delay;
        }

        @Override
        public String getJobId() {
            return jobId;
        }

        @Override
        public Schedule getSchedule() {
            return Schedule.withDelay(delay);
        }

        @Override
        public void run(DistributedJobContext context) throws Exception {

        }

        @Override
        public long getInitialJobDelay() {
            return delay;
        }

        @Override
        public WorkPool getWorkPool() {
            return workPool;
        }

        @Override
        public WorkPoolRunningStrategy getWorkPoolRunningStrategy() {
            return WorkPoolRunningStrategies.getSingleThreadStrategy();
        }

        @Override
        public long getWorkPoolCheckPeriod() {
            return 100;
        }
    }

    public static class CustomAssignmentStrategy implements AssignmentStrategy {

        @Override
        public ZookeeperState reassignAndBalance(
                ZookeeperState availability,
                ZookeeperState prevAssignment,
                ZookeeperState currentAssignment,
                Map<JobId, List<WorkItem>> itemsToAssign) {


            AssignmentStrategyFactory.RENDEZVOUS.reassignAndBalance(
                    availability, prevAssignment, currentAssignment, itemsToAssign
            );

            AssignmentStrategyFactory.EVENLY_SPREAD.reassignAndBalance(
                    availability, prevAssignment, currentAssignment, itemsToAssign
            );

            return currentAssignment;
        }
    }

    @Test
    public void example() throws Exception {
        initDjm();

    }

    private WorkPool createWorkPool(String jobId, int workItemsNumber) {
        Set<String> workPool = new HashSet<>();

        for (int i = 0; i < workItemsNumber; i++) {
            workPool.add(jobId + ".work-item-" + i);
        }

        return WorkPool.of(workPool);
    }

    private List<DistributedJobManager> createDjmPool(int count) throws Exception {
        List<DistributedJobManager> distributedJobManagers = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            DistributedJobManager djm = new DistributedJobManager(
                    "worker-" + i,
                    zkTestingServer.createClient(),
                    "/root/path",
                    Arrays.asList(
                            new StubbedMultiJob(1, createWorkPool("distr-job-id-1", 2).getItems(), 500L),
                            new StubbedMultiJob(2, createWorkPool("distr-job-id-2", 3).getItems(), 0L),
                            new StubbedMultiJob(3, createWorkPool("distr-job-id-3", 1).getItems(), 0L)),
                    AssignmentStrategyFactory.RENDEZVOUS,
                    new AggregatingProfiler(),
                    DynamicProperty.of(10_000L),
                    DynamicProperty.of(true)
            );
            distributedJobManagers.add(djm);
        }
        return distributedJobManagers;
    }


    private void initEmptyDjms(int countDjms) throws Exception {
        for (int i = 0; i < countDjms; i++) {
            DistributedJobManager djm = new DistributedJobManager(
                    "worker-" + i,
                    zkTestingServer.createClient(),
                    "/root/path",
                    Collections.emptyList(),
                    AssignmentStrategyFactory.EVENLY_SPREAD,
                    new AggregatingProfiler(),
                    DynamicProperty.of(10_000L),
                    DynamicProperty.of(true)
            );
        }
    }

    private void initDjm() throws Exception {
        DistributedJobManager djm = new DistributedJobManager(
                "worker-" + 0,
                zkTestingServer.createClient(),
                "/root/path",
                Arrays.asList(
                        new StubbedMultiJob(0, createWorkPool("distr-job-id-0", 1).getItems(), 5000L),
                        new StubbedMultiJob(1, createWorkPool("distr-job-id-1", 6).getItems(), 5000L),
                        new StubbedMultiJob(2, createWorkPool("distr-job-id-2", 2).getItems(), 5000L)
                ),
                AssignmentStrategyFactory.EVENLY_SPREAD,
                new AggregatingProfiler(),
                DynamicProperty.of(10_000L),
                DynamicProperty.of(true)
        );

        DistributedJobManager djm1 = new DistributedJobManager(
                "worker-" + 1,
                zkTestingServer.createClient(),
                "/root/path",
                Arrays.asList(),
                AssignmentStrategyFactory.EVENLY_SPREAD,
                new AggregatingProfiler(),
                DynamicProperty.of(10_000L),
                DynamicProperty.of(true)
        );

        DistributedJobManager djm2 = new DistributedJobManager(
                "worker-" + 2,
                zkTestingServer.createClient(),
                "/root/path",
                Arrays.asList(),
                AssignmentStrategyFactory.EVENLY_SPREAD,
                new AggregatingProfiler(),
                DynamicProperty.of(10_000L),
                DynamicProperty.of(true)
        );

//        Thread.sleep(3000);
    }
}