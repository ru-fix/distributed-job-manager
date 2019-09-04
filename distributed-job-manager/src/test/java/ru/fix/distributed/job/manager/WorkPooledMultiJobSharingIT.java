package ru.fix.distributed.job.manager;

import org.apache.curator.framework.CuratorFramework;
import org.junit.jupiter.api.Test;
import ru.fix.aggregating.profiler.AggregatingProfiler;
import ru.fix.distributed.job.manager.strategy.AssignmentStrategy;
import ru.fix.distributed.job.manager.strategy.AssignmentStrategyFactory;
import ru.fix.dynamic.property.api.DynamicProperty;
import ru.fix.stdlib.concurrency.threads.Schedule;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.Mockito.*;

public class WorkPooledMultiJobSharingIT extends AbstractJobManagerTest {

    private WorkItemMonitor monitor = mock(WorkItemMonitor.class);

    private final String serverId = Byte.toString(Byte.MAX_VALUE);

    @SuppressWarnings("unchecked")
    @Test
    public void shouldRunAllWorkItemsInSingleWorker() throws Exception {
        try (CuratorFramework curator = zkTestingServer.createClient();
             DistributedJobManager ignored = new DistributedJobManager(
                     "work-name",
                     curator,
                     JOB_MANAGER_ZK_ROOT_PATH,
                     new HashSet<>(Collections.singletonList(
                             new SingleThreadMultiJob(
                                     new HashSet<>(Arrays.asList("1", "2", "3", "4"))))),
                     AssignmentStrategyFactory.DEFAULT,
                     new AggregatingProfiler(),
                     getTerminationWaitTime())
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
        public Schedule getSchedule() {
            return Schedule.withDelay(100L);
        }

        @Override
        public String getJobId() {
            return "job-id";
        }

        @Override
        public void run(DistributedJobContext context) {
            monitor.check(context.getWorkShare());
        }
    }
}
