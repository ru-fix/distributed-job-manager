package ru.fix.cpapsm.commons.distributed.job.manager;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.junit.Test;
import ru.fix.aggregating.profiler.AggregatingProfiler;
import ru.fix.stdlib.concurrency.threads.Schedule;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class WorkShareLockServiceTest extends AbstractJobManagerTest {

    private final String serverId = Byte.toString(Byte.MAX_VALUE);

    /**
     * количество слушателей должно быть одинаковым до и после добавления и релиз 100 джоб
     *
     * @throws Exception
     */
    @Test
    public void shouldCorrectRelease() throws Exception {
        final String workerName = "worker-1";
        final String rootPath = "/test";
        try (
                CuratorFramework curator = zkTestingServer.createClient();
                WorkShareLockServiceImpl workShareLockService = new WorkShareLockServiceImpl(curator, new JobManagerPaths
                        (rootPath), workerName, serverId, new AggregatingProfiler())
        ) {
            ListenerContainer<ConnectionStateListener> listenable =
                    (ListenerContainer<ConnectionStateListener>) curator.getConnectionStateListenable();
            int sizeBefore = listenable.size();
            for (int i = 0; i < 100; ++i) {
                SimpleJob job = new SimpleJob();
                workShareLockService.tryAcquire(job, "simple", () -> {
                });
                workShareLockService.release(job, "simple");
            }
            workShareLockService.close();
            ListenerContainer<ConnectionStateListener> listenable2 =
                    (ListenerContainer<ConnectionStateListener>) curator.getConnectionStateListenable();
            int size = listenable2.size();
            assertEquals(sizeBefore, size);
            curator.close();
        }
    }

    @Test
    public void hasAcquiredLock_beforeAndAfterAcquire() {
        final String workerName = "worker-1";
        final String rootPath = "/test";
        try (
                CuratorFramework curator = zkTestingServer.createClient();
                WorkShareLockServiceImpl workShareLockService = new WorkShareLockServiceImpl(curator,
                        new JobManagerPaths(rootPath), workerName, serverId, new AggregatingProfiler())
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
        public Schedule getSchedule() {
            return Schedule.withDelay(TimeUnit.SECONDS.toMillis(1));
        }

        /**
         * Method will be invoked on one of cluster machines
         */
        @Override
        public void run(DistributedJobContext context) throws Exception {
        }


        /**
         * See {@link ru.fix.cpapsm.commons.distributed.job.manager.util.WorkPoolUtils#checkWorkPoolItemsRestrictions}
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

    }

}