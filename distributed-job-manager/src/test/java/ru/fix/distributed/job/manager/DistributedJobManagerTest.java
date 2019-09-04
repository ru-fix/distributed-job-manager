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
    private static final String JOB_MANAGER_ZK_ROOT_PATH = "/djm/job-manager-test";
    private ZKTestingServer zkTestingServer;
    private final String serverId = Byte.toString(Byte.MAX_VALUE);

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
            return 0;
        }
    }

    public static class CustomAssignmentStrategy implements AssignmentStrategy {

        @Override
        public void reassignAndBalance(
                ZookeeperState availability,
                ZookeeperState prevAssignment,
                ZookeeperState newAssignment,
                Map<JobId, List<WorkItem>> itemsToAssign) {

            /*// get all work items for rebill job from itemsToAssign
            Map<JobId, List<WorkItem>> itemsToRebillJob = null;
            AssignmentStrategyFactory.EVENLY_SPREAD.reassignAndBalance(
                    availability, prevAssignment, newAssignment, itemsToRebillJob);
            // remove rebill job from itemsToAssign
            itemsToAssign.remove(new JobId("rebill-job"));

            // get all work items for ussd job from itemsToAssign
            Map<JobId, List<WorkItem>> itemsToUssdJob = null;
            // here we implements custom logic for ussd job...
            // remove ussd job from itemsToAssign
            itemsToAssign.remove(new JobId("ussd-job"));

            // get all work items for sms job from itemsToAssign
            Map<JobId, List<WorkItem>> itemsToSmsJob = null;
            // here we implements custom logic for ussd job...
            // remove ussd job from itemsToAssign
            itemsToAssign.remove(new JobId("ussd-job"));

            // get all work items for other jobs from itemsToAssign
            Map<JobId, List<WorkItem>> itemsToOtherJobs = null;*/
            AssignmentStrategyFactory.RENDEZVOUS.reassignAndBalance(
                    availability, prevAssignment, newAssignment, itemsToAssign
            );
        }
    }

    @Test
    public void example() throws Exception {
        StubbedMultiJob testJob = new StubbedMultiJob(1, Set.of("11", "22", "33"));

        DistributedJobManager distributedJobManager = new DistributedJobManager(
                "worker-1",
                zkTestingServer.createClient(),
                "/root/path",
                Arrays.asList(
                        new DistributedJobStub("sms-job", createWorkPool("sms-job", 2), 2000L),
                        new DistributedJobStub("ussd-job", createWorkPool("ussd-job", 3)),
                        new DistributedJobStub("rebill-job", createWorkPool("rebill-job", 3))),
                AssignmentStrategyFactory.RENDEZVOUS,
                new AggregatingProfiler(),
                DynamicProperty.of(10_000L),
                DynamicProperty.of(true)
        );

        DistributedJobManager distributedJobManager2 = new DistributedJobManager(
                "worker-2",
                zkTestingServer.createClient(),
                "/root/path",
                Arrays.asList(
                        testJob,
                        new DistributedJobStub("custom-job-1", createWorkPool("custom-job-1", 2), 500L),
                        new DistributedJobStub("custom-job-2", createWorkPool("custom-job-2", 3), 0L),
                        new DistributedJobStub("custom-job-3", createWorkPool("custom-job-3", 1), 0L)),
                AssignmentStrategyFactory.RENDEZVOUS,
                new AggregatingProfiler(),
                DynamicProperty.of(10_000L),
                DynamicProperty.of(true)
        );

        testJob.updateWorkPool(Set.of("44", "33"));

    }

    private WorkPool createWorkPool(String jobId, int workItemsNumber) {
        Set<String> workPool = new HashSet<>();

        for (int i = 0; i < workItemsNumber; i++) {
            workPool.add(jobId + ".work-item-" + i);
        }

        return WorkPool.of(workPool);
    }
}