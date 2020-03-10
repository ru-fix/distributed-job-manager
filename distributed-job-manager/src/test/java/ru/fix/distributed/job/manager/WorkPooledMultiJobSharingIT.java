package ru.fix.distributed.job.manager;

import org.apache.curator.framework.CuratorFramework;
import org.junit.jupiter.api.Test;
import ru.fix.aggregating.profiler.AggregatingProfiler;
import ru.fix.distributed.job.manager.model.DistributedJobManagerSettings;
import ru.fix.distributed.job.manager.strategy.AssignmentStrategies;
import ru.fix.dynamic.property.api.DynamicProperty;
import ru.fix.stdlib.concurrency.threads.Schedule;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.Mockito.*;

class WorkPooledMultiJobSharingIT extends AbstractJobManagerTest {

    private WorkItemMonitor monitor = mock(WorkItemMonitor.class);

    @Test
    void shouldRunAllWorkItemsInSingleWorker() throws Exception {
        try (CuratorFramework curator = zkTestingServer.createClient();
             DistributedJobManager ignored = new DistributedJobManager(
                     curator,
                     new HashSet<>(Collections.singletonList(
                             new SingleThreadMultiJob(
                                     new HashSet<>(Arrays.asList("1", "2", "3", "4"))))),
                     new AggregatingProfiler(),
                     new DistributedJobManagerSettings(
                             "work-name",
                             JOB_MANAGER_ZK_ROOT_PATH,
                             AssignmentStrategies.Companion.getDEFAULT(),
                             getTerminationWaitTime()
                     )
             )
        ) {
            verify(monitor, timeout(10_000)).check(anySet());
        }
    }

    private DynamicProperty<Long> getTerminationWaitTime() {
        return DynamicProperty.of(180_000L);
    }

    private class SingleThreadMultiJob implements DistributedJob {

        private final Set<String> workerPool;

        SingleThreadMultiJob(Set<String> workerPool) {
            this.workerPool = workerPool;
        }

        @Override
        public WorkPool getWorkPool() {
            return WorkPool.of(workerPool);
        }

        @Override
        public WorkPoolRunningStrategy getWorkPoolRunningStrategy() {
            return WorkPoolRunningStrategies.getSingleThreadStrategy();
        }

        @Override
        public DynamicProperty<Schedule> getSchedule() {
            return Schedule.withDelay(DynamicProperty.of(100L));
        }

        @Override
        public String getJobId() {
            return "job-id";
        }

        @Override
        public void run(DistributedJobContext context) {
            monitor.check(context.getWorkShare());
        }

        @Override
        public long getWorkPoolCheckPeriod() {
            return 0;
        }

    }
}
