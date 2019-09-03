package ru.fix.distributed.job.manager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.fix.aggregating.profiler.AggregatingProfiler;
import ru.fix.distributed.job.manager.model.distribution.JobId;
import ru.fix.distributed.job.manager.model.distribution.WorkItem;
import ru.fix.distributed.job.manager.model.distribution.ZookeeperState;
import ru.fix.distributed.job.manager.strategy.AssignmentStrategy;
import ru.fix.distributed.job.manager.strategy.AssignmentStrategyFactory;
import ru.fix.dynamic.property.api.DynamicProperty;
import ru.fix.stdlib.concurrency.threads.Schedule;
import ru.fix.zookeeper.testing.ZKTestingServer;

import java.util.List;
import java.util.Map;

class DistributedJobManagerTest {
    private static final String JOB_MANAGER_ZK_ROOT_PATH = "/djm/job-manager-test";
    private ZKTestingServer zkTestingServer;
    private final String serverId = Byte.toString(Byte.MAX_VALUE);

    @BeforeEach
    public void setUp() throws Exception {
        zkTestingServer = new ZKTestingServer();
        zkTestingServer.start();
    }

    public static class RebillJob implements DistributedJob {
        @Override
        public String getJobId() {
            return null;
        }

        @Override
        public Schedule getSchedule() {
            return null;
        }

        @Override
        public void run(DistributedJobContext context) throws Exception {

        }

        @Override
        public long getInitialJobDelay() {
            return 0;
        }

        @Override
        public WorkPool getWorkPool() {
            return null;
        }

        @Override
        public WorkPoolRunningStrategy getWorkPoolRunningStrategy() {
            return null;
        }

        @Override
        public long getWorkPoolCheckPeriod() {
            return 0;
        }
    }

    public static class SmsJob implements DistributedJob {
        @Override
        public String getJobId() {
            return null;
        }

        @Override
        public Schedule getSchedule() {
            return null;
        }

        @Override
        public void run(DistributedJobContext context) throws Exception {

        }

        @Override
        public long getInitialJobDelay() {
            return 0;
        }

        @Override
        public WorkPool getWorkPool() {
            return null;
        }

        @Override
        public WorkPoolRunningStrategy getWorkPoolRunningStrategy() {
            return null;
        }
    }

    public static class UssdJob implements DistributedJob {
        @Override
        public String getJobId() {
            return null;
        }

        @Override
        public Schedule getSchedule() {
            return null;
        }

        @Override
        public void run(DistributedJobContext context) throws Exception {

        }

        @Override
        public long getInitialJobDelay() {
            return 0;
        }

        @Override
        public WorkPool getWorkPool() {
            return null;
        }

        @Override
        public WorkPoolRunningStrategy getWorkPoolRunningStrategy() {
            return null;
        }
    }

    public static class CustomAssignmentStrategy implements AssignmentStrategy {

        @Override
        public void reassignAndBalance(
                ZookeeperState availability,
                ZookeeperState prevAssignment,
                ZookeeperState newAssignment,
                Map<JobId, List<WorkItem>> itemsToAssign) {

            // get all work items for rebill job from itemsToAssign
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
            Map<JobId, List<WorkItem>> itemsToOtherJobs = null;
            AssignmentStrategyFactory.RENDEZVOUS.reassignAndBalance(
                   availability, prevAssignment, newAssignment, itemsToOtherJobs
            );
        }
    }

    @Test
    public void example() {
        try {
            DistributedJobManager distributedJobManager = new DistributedJobManager(
                    "application-id",
                    new ZKTestingServer().createClient(),
                    "/root/path",
                    List.of(new SmsJob(), new UssdJob(), new RebillJob()),
                    new CustomAssignmentStrategy(),
                    new AggregatingProfiler(),
                    DynamicProperty.of(10000L)
            );
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}