package ru.fix.distributed.job.manager;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.ListenerManager;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.junit.jupiter.api.Test;
import ru.fix.aggregating.profiler.AggregatingProfiler;
import ru.fix.dynamic.property.api.DynamicProperty;
import ru.fix.stdlib.concurrency.threads.Schedule;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class WorkShareLockServiceTest extends AbstractJobManagerTest {

    /**
     * количество слушателей должно быть одинаковым до и после добавления и релиз 100 джоб
     *
     * @throws Exception
     */
    @Test
    public void shouldCorrectRelease() throws Exception {
        final String nodeId = "worker-1";
        final String rootPath = "/test";
        try (
                CuratorFramework curator = zkTestingServer.createClient();
                WorkShareLockServiceImpl workShareLockService = new WorkShareLockServiceImpl(curator, new JobManagerPaths
                        (rootPath), nodeId, new AggregatingProfiler())
        ) {
            ListenerManager<ConnectionStateListener, ConnectionStateListener> listenableBefore =
                    (ListenerManager<ConnectionStateListener, ConnectionStateListener>) curator.getConnectionStateListenable();
            int sizeBefore = listenableBefore.size();
            for (int i = 0; i < 100; ++i) {
                SimpleJob job = new SimpleJob();
                workShareLockService.tryAcquire(job, "simple", () -> {
                });
                workShareLockService.release(job, "simple");
            }
            workShareLockService.close();
            ListenerManager<ConnectionStateListener, ConnectionStateListener> listenableAfter =
                    (ListenerManager<ConnectionStateListener, ConnectionStateListener>) curator.getConnectionStateListenable();
            int sizeAfter = listenableAfter.size();
            assertEquals(sizeBefore, sizeAfter);
        }
    }

    @Test
    public void hasAcquiredLock_beforeAndAfterAcquire() {
        final String nodeId = "worker-1";
        final String rootPath = "/test";
        try (
                CuratorFramework curator = zkTestingServer.createClient();
                WorkShareLockServiceImpl workShareLockService = new WorkShareLockServiceImpl(curator,
                        new JobManagerPaths(rootPath), nodeId, new AggregatingProfiler())
        ) {
            boolean beforeAcquire = workShareLockService.existsLock(new SimpleJob(), "item");
            assertThat(beforeAcquire, is(false));

            final SimpleJob job = new SimpleJob();
            final String workItem = "all";
            workShareLockService.tryAcquire(job, workItem, () -> {
            });

            boolean afterAcquire = workShareLockService.existsLock(job, workItem);
            assertThat(afterAcquire, is(true));

            workShareLockService.release(job, workItem);

            boolean afterRelease = workShareLockService.existsLock(job, workItem);
            assertThat(afterRelease, is(false));
        }
    }


    class SimpleJob implements DistributedJob {
        /**
         * @return id of the job.
         */
        @Override
        public String getJobId() {
            return "job";
        }

        /**
         * @return delay between job invocation
         */
        @Override
        public DynamicProperty<Schedule> getSchedule() {
            return Schedule.withDelay(DynamicProperty.of(TimeUnit.SECONDS.toMillis(1)));
        }

        /**
         * Method will be invoked on one of cluster machines
         */
        @Override
        public void run(DistributedJobContext context) throws Exception {
        }


        /**
         * See {@link ru.fix.distributed.job.manager.util.WorkPoolUtils#checkWorkPoolItemsRestrictions}
         * for restrictions on WorkPool items
         */
        @Override
        public WorkPool getWorkPool() {
            return WorkPool.of(Collections.singleton("all"));
        }

        /**
         * Определеяет возможность запуска каждой
         */
        @Override
        public WorkPoolRunningStrategy getWorkPoolRunningStrategy() {
            return WorkPoolRunningStrategies.getSingleThreadStrategy();
        }

        @Override
        public long getWorkPoolCheckPeriod() {
            return 0;
        }

    }

}
