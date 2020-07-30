package ru.fix.distributed.job.manager;

import org.apache.curator.framework.CuratorFramework;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import ru.fix.dynamic.property.api.AtomicProperty;
import ru.fix.dynamic.property.api.DynamicProperty;
import ru.fix.stdlib.concurrency.threads.Schedule;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.mockito.Mockito.*;

class WorkPooledMultiJobSharingIT extends AbstractJobManagerTest {

    @SuppressWarnings("unchecked")
    private final Consumer<Set<String>> monitor = mock(Consumer.class);

    @Test
    void shouldRunAllWorkItemsInSingleWorker() throws Exception {
        try (CuratorFramework curator = defaultZkClient();
             DistributedJobManager ignored = createNewJobManager(
                     Collections.singleton(
                             new SingleThreadMultiJob(
                                     Schedule.withDelay(DynamicProperty.of(100L)),
                                     new HashSet<>(Arrays.asList("1", "2", "3", "4"))
                             )
                     ),
                     curator
             )
        ) {
            verify(monitor, timeout(10_000)).accept(anySet());
        }
    }

    @Test
    @Timeout(20)
    void delayedJobShouldStartAccordingToNewScheduleSettings() throws Exception {
        // initial setting - 1h delay, and implicit 1h initial delay
        AtomicProperty<Long> delay = new AtomicProperty<>(TimeUnit.HOURS.toMillis(1));
        try (CuratorFramework curator = defaultZkClient();
             DistributedJobManager ignored = createNewJobManager(
                     Collections.singleton(
                             new SingleThreadMultiJob(
                                     Schedule.withDelay(delay),
                                     new HashSet<>(Arrays.asList("1", "2", "3", "4"))
                             )
                     ),
                     curator
             )
        ) {
            // initial 1h delay continues still, job not started
            verify(monitor, after(3000).never()).accept(anySet());

            // change schedule delay setting of the job with implicit start delay settings,
            // so the job should start in moments
            delay.set(TimeUnit.SECONDS.toMillis(1L));

            verify(monitor, timeout(5_000)).accept(anySet());
        }
    }

    @Test
    @Timeout(20)
    void delayedJobShouldStartAccordingToNewInitialDelaySetting() throws Exception {
        // initial setting - 1h delay, and explicit 1h initial delay
        long delay1H = TimeUnit.HOURS.toMillis(1);
        AtomicProperty<Long> startDelay = new AtomicProperty<>(delay1H);
        try (CuratorFramework curator = defaultZkClient();
             DistributedJobManager ignored = createNewJobManager(
                     Collections.singleton(
                             new CustomInitialDelayImplJob(
                                     Schedule.withDelay(DynamicProperty.of(delay1H)),
                                     startDelay,
                                     new HashSet<>(Arrays.asList("1", "2", "3", "4"))
                             )
                     ),
                     curator
             )
        ) {
            // initial 1h delay continues still, job not started
            verify(monitor, after(3000).never()).accept(anySet());

            // change start delay setting, so the job should start immediately
            startDelay.set(0L);

            verify(monitor, timeout(5_000)).accept(anySet());
        }
    }

    private class SingleThreadMultiJob implements DistributedJob {

        private final DynamicProperty<Schedule> schedule;
        private final Set<String> workerPool;

        SingleThreadMultiJob(DynamicProperty<Schedule> schedule, Set<String> workerPool) {
            this.schedule = schedule;
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
            return schedule;
        }

        @Override
        public JobId getJobId() {
            return new JobId("job-id");
        }

        @Override
        public void run(DistributedJobContext context) {
            monitor.accept(context.getWorkShare());
        }

        @Override
        public long getWorkPoolCheckPeriod() {
            return 0L;
        }

    }

    private class CustomInitialDelayImplJob implements DistributedJob {

        private final DynamicProperty<Schedule> schedule;
        private final DynamicProperty<Long> startDelay;
        private final Set<String> workerPool;

        CustomInitialDelayImplJob(DynamicProperty<Schedule> schedule,
                                  DynamicProperty<Long> startDelay,
                                  Set<String> workerPool) {
            this.schedule = schedule;
            this.startDelay = startDelay;
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
            return schedule;
        }

        @Override
        public DynamicProperty<Long> getInitialJobDelay() {
            return startDelay;
        }

        @Override
        public JobId getJobId() {
            return new JobId("job-id");
        }

        @Override
        public void run(DistributedJobContext context) {
            monitor.accept(context.getWorkShare());
        }

        @Override
        public long getWorkPoolCheckPeriod() {
            return 0L;
        }
    }
}
