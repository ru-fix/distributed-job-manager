package ru.fix.distributed.job.manager;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.aggregating.profiler.AggregatingProfiler;
import ru.fix.stdlib.concurrency.threads.NamedExecutors;
import ru.fix.zookeeper.lock.PersistentExpiringDistributedLock;
import ru.fix.zookeeper.testing.ZKTestingServer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.*;

public class PersistentExpiringDistributedLockIT {

    private static final Logger logger = LoggerFactory.getLogger(PersistentExpiringDistributedLockIT.class);

    private final String serverId = Byte.toString(Byte.MAX_VALUE);

    public ZKTestingServer zkTestingServer;

    @Before
    public void setUp() throws Exception {
        zkTestingServer = new ZKTestingServer();
        zkTestingServer.start();
    }

    @Test(timeout = 10_000)
    public void test_lock_release() throws Exception {
        ExecutorService notificationsExecutor = NamedExecutors.newSingleThreadPool(
                "PersistentExpiringDistributedLock-Notifications-",
                new AggregatingProfiler());
        PersistentExpiringDistributedLock lock0 = new PersistentExpiringDistributedLock(zkTestingServer.getClient(),
                notificationsExecutor, "0", "/lockingNode", serverId);
        PersistentExpiringDistributedLock lock1 = new PersistentExpiringDistributedLock(zkTestingServer.getClient(),
                notificationsExecutor, "1", "/lockingNode", serverId);

        try {
            lock0.expirableAcquire(100_000, 100);
        } finally {
            lock0.release();
        }

        try {
            boolean acquired = lock1.expirableAcquire(100_000, 100);
            assertTrue(acquired, "Lock1 is not acquired (timeout expiration error)");
        } finally {
            lock1.release();
        }
    }

    @Test(timeout = 10_000)
    public void deleting_node_if_lock_release() throws Exception {
        ExecutorService notificationsExecutor =
                NamedExecutors.newSingleThreadPool("PersistentExpiringDistributedLock-Notifications-",
                        new AggregatingProfiler());
        PersistentExpiringDistributedLock lock1 = new PersistentExpiringDistributedLock(
                zkTestingServer.getClient(), notificationsExecutor, "1", "/lockingNode",
                serverId);

        try {
            boolean acquired = lock1.expirableAcquire(100_000, 100);
            assertTrue(acquired, "Lock1 is not acquired (timeout expiration error)");
        } finally {
            lock1.release();
        }
        assertNull(zkTestingServer.getClient().checkExists().forPath("/lockingNode"));
    }

    @Test(timeout = 20_000)
    public void test_lock_prolong_after_timeout() throws Exception {
        ExecutorService notificationsExecutor = NamedExecutors.newSingleThreadPool
                ("PersistentExpiringDistributedLock-Notifications-", new AggregatingProfiler());
        PersistentExpiringDistributedLock lock0 = new PersistentExpiringDistributedLock(zkTestingServer.getClient(),
                notificationsExecutor, "0", "/lockingNode", serverId);
        PersistentExpiringDistributedLock lock1 = new PersistentExpiringDistributedLock(zkTestingServer.getClient(),
                notificationsExecutor, "1", "/lockingNode", serverId);

        try {
            lock0.expirableAcquire(1000, 100);

            Thread.sleep(1000);

            boolean acquired = lock1.checkAndProlong(100);
            assertFalse(acquired, "Lock1 is acquired after timeout");
        } finally {
            lock0.release();
            lock1.release();
        }
    }

    @Test
    public void test_lock_prolong() throws Exception {
        long lock0Timeout = 2_000;
        int renewCount = 10;

        ExecutorService notificationsExecutor = NamedExecutors.newSingleThreadPool
                ("PersistentExpiringDistributedLock-Notifications-", new AggregatingProfiler());
        PersistentExpiringDistributedLock lock0 = new PersistentExpiringDistributedLock(zkTestingServer.getClient(),
                notificationsExecutor, "0", "/lockingNode", serverId);

        try {
            lock0.expirableAcquire(lock0Timeout, 0);

            for (int i = 0; i < renewCount; i++) {
                Thread.sleep(lock0Timeout / 2);
                boolean renewed = lock0.checkAndProlong(lock0Timeout);
                assertTrue(renewed, "Failed to prolong lock at " + i + " element; timeout is " + lock0Timeout);
            }
        } finally {
            lock0.release();
        }
    }

    @Test
    public void test_lock_renew_after_expiration() throws Exception {
        long lock0Timeout = 1000;

        ExecutorService notificationsExecutor = NamedExecutors.newSingleThreadPool(
                "PersistentExpiringDistributedLock-Notifications-", new AggregatingProfiler());
        PersistentExpiringDistributedLock lock0 = new PersistentExpiringDistributedLock(zkTestingServer.getClient(),
                notificationsExecutor, "0", "/lockingNode", serverId);

        try {
            lock0.expirableAcquire(lock0Timeout, 0);
            Thread.sleep(lock0Timeout);
            boolean renewed = lock0.expirableAcquire(1_000, 100);
            assertTrue(renewed, "Renew after timeout expiration failed");
        } finally {
            lock0.release();
        }
    }

    @Test(timeout = 120_000)
    public void test_lock_complex_multi_thread() throws InterruptedException {
        int threadsCount = 3;
        int threadIncrementLoop = 5;

        AtomicInteger releaseCounter = new AtomicInteger();
        AtomicInteger counter = new AtomicInteger();

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < threadsCount; i++) {
            String lockId = String.valueOf(i);
            Thread incrementedThread = new Thread(() -> {
                try {
                    int j = 0;

                    while (j < threadIncrementLoop) {
                        PersistentExpiringDistributedLock lock = null;
                        ExecutorService notificationsExecutor = NamedExecutors.newSingleThreadPool
                                ("PersistentExpiringDistributedLock-Notifications-", new AggregatingProfiler());
                        try {
                            lock = new PersistentExpiringDistributedLock(zkTestingServer.getClient(),
                                    notificationsExecutor, lockId, "/lockingNode", serverId);
                            if (lock.expirableAcquire(5_000, 1)) {
                                int value = counter.get();
                                if (lock.checkAndProlong(5_000)) {
                                    if (!counter.compareAndSet(value, value + 1)) {
                                        logger.info("CAS failed - concurrent modification of counter");
                                    }
                                    j++;
                                }

                                if (counter.get() % 30 == 0) {
                                    logger.info("Current counter value {}", counter.get());
                                }
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        } finally {
                            try {
                                if (lock != null) {
                                    lock.close();
                                }
                            } catch (Exception e) {
                                // ignore this
                            }
                            notificationsExecutor.shutdown();
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            incrementedThread.start();
            threads.add(incrementedThread);
        }

        for (Thread thread : threads) {
            thread.join();
        }

        assertEquals(threadsCount * threadIncrementLoop, counter.get());
    }

    @Test(timeout = 10_000)
    public void test_acquire_after_acquire() throws Exception {
        long lock0Timeout = 100_000;
        long lock1Timeout = 100_000;
        long tryTimeout = 100;

        ExecutorService notificationsExecutor = NamedExecutors.newSingleThreadPool
                ("PersistentExpiringDistributedLock-Notifications-", new AggregatingProfiler());
        PersistentExpiringDistributedLock lock0 = new PersistentExpiringDistributedLock(zkTestingServer.getClient(),
                notificationsExecutor, "0", "/lockingNode", serverId);
        PersistentExpiringDistributedLock lock1 = new PersistentExpiringDistributedLock(zkTestingServer.getClient(),
                notificationsExecutor, "1", "/lockingNode", serverId);

        try {
            lock0.expirableAcquire(lock0Timeout, 0);
            lock0.expirableAcquire(lock0Timeout, 0);
        } finally {
            lock0.release();
        }

        try {
            boolean acquired = lock1.expirableAcquire(lock1Timeout, tryTimeout);
            assertTrue(acquired, "Lock1 is not acquired (timeout expiration error)");
        } finally {
            lock0.release();
        }
    }

    @Test
    @SuppressWarnings("squid:S3415")
    //https://groups.google.com/forum/?nomobile=true#!msg/sonarqube/E48APX81Rmg/Xd92b4y3AQAJ
    public void test_lock_acquire_with_timeout() throws Exception {
        long lock0Timeout = 100_000;
        long lock1Timeout = 10;

        ExecutorService notificationsExecutor = NamedExecutors.newSingleThreadPool
                ("PersistentExpiringDistributedLock-Notifications-", new AggregatingProfiler());
        PersistentExpiringDistributedLock lock0 = new PersistentExpiringDistributedLock(zkTestingServer.getClient(),
                notificationsExecutor, "0", "/lockingNode", serverId);
        PersistentExpiringDistributedLock lock1 = new PersistentExpiringDistributedLock(zkTestingServer.getClient(),
                notificationsExecutor, "1", "/lockingNode", serverId);

        try {
            lock0.expirableAcquire(lock0Timeout, 0);

            long startedTime = System.currentTimeMillis();
            boolean acquired = lock1.expirableAcquire(lock1Timeout, 5_000);
            long completedTime = System.currentTimeMillis();

            assertFalse(acquired, "Lock1 is acquired");
            assertEquals(5000, completedTime - startedTime, 500);
        } finally {
            lock0.release();
        }
    }

    @Test
    public void test_lock_prolong_if_expires_in() throws Exception {
        long lock0Timeout = 2_000;
        int renewCount = 15;

        ExecutorService notificationsExecutor = NamedExecutors.newSingleThreadPool
                ("PersistentExpiringDistributedLock-Notifications-", new AggregatingProfiler());
        PersistentExpiringDistributedLock lock0 = new PersistentExpiringDistributedLock(zkTestingServer.getClient(),
                notificationsExecutor, "0", "/lockingNode", serverId);

        try {
            lock0.expirableAcquire(lock0Timeout, 0);

            for (int i = 0; i < renewCount; i++) {
                Thread.sleep(lock0Timeout / 3);
                boolean renewed = lock0.checkAndProlongIfExpiresIn(lock0Timeout, 1_000);
                assertTrue(renewed, "Failed to prolong lock at " + i + " element; timeout is " + lock0Timeout);
            }
        } finally {
            lock0.release();
        }
    }

    @Test
    public void test_lock_prolong_if_expires_in_call_count() throws Exception {
        ExecutorService notificationsExecutor = NamedExecutors.newSingleThreadPool
                ("PersistentExpiringDistributedLock-Notifications-", new AggregatingProfiler());
        PersistentExpiringDistributedLock lock0 = new PersistentExpiringDistributedLock(zkTestingServer.getClient(),
                notificationsExecutor, "0", "/lockingNode", serverId);
        PersistentExpiringDistributedLock lock1 = new PersistentExpiringDistributedLock(zkTestingServer.getClient(),
                notificationsExecutor, "1", "/lockingNode", serverId);

        try {
            lock0.expirableAcquire(3_000, 0);
            lock0.checkAndProlongIfExpiresIn(25_000, 500);
            Thread.sleep(3_000);

            boolean acquiredLockBySecondInstance = lock1.expirableAcquire(5_000, 100);
            assertTrue(acquiredLockBySecondInstance,
                    "Lock1 failed to acquire the lock (lock0#checkAndProlongIfExpiresIn did prolong)");
        } finally {
            lock0.release();
            lock1.release();
        }
    }

    /**
     * Логика теста. Если при acquire лока произошло такое, что он уже занят, мы ждем
     * некоторое время, пока его не отпустит владелец.
     * <p>
     * В тесте 2 лока попеременно пытаются взять лок и отпустить его. В итоге на методе
     * acquire должна происходить блокировка, которая должна окончится при релизе другого лока
     */
    @Test(timeout = 5_000)
    public void continueAcquireAfterLockReleased() throws Exception {
        ScheduledExecutorService backgroundExecutor = Executors.newSingleThreadScheduledExecutor();

        ExecutorService notificationsExecutor = NamedExecutors.newSingleThreadPool
                ("PersistentExpiringDistributedLock-Listeners-", new AggregatingProfiler());

        try (PersistentExpiringDistributedLock lock0 = new PersistentExpiringDistributedLock(zkTestingServer.getClient(),
                notificationsExecutor, "A", "/lockingNode", serverId);
             PersistentExpiringDistributedLock lock1 = new PersistentExpiringDistributedLock(zkTestingServer.getClient(),
                     notificationsExecutor, "B", "/lockingNode", serverId)) {
            if (!lock0.expirableAcquire(15_000, 1000)) {
                fail("Can't acquire first lock");
            }
            backgroundExecutor.execute(() -> {
                try {
                    final long lock1Start = System.currentTimeMillis();
                    if (!lock1.expirableAcquire(15_000, 10_000)) {
                        fail("Second lock must acquire zk node");
                    }
                    long actualAcqPeriod = System.currentTimeMillis() - lock1Start;
                    logger.info("Second lock acquired for {} ms", actualAcqPeriod);
                    assertThat(actualAcqPeriod, lessThan(10_000L));
                } catch (Exception e) {
                    logger.error("Can't acquire second lock when first was released", e);
                    fail(e.getMessage());
                }
            });

            Thread.sleep(1000);
            lock0.release();

            backgroundExecutor.execute(() -> {
                try {
                    Thread.sleep(1000);
                    lock1.release();
                } catch (Exception e) {
                    logger.error("Can't release the second lock", e);
                    fail("Something wrong when second lock try to release");
                }
            });
            final long acqStartTimeSnapshot = System.currentTimeMillis();
            if (!lock0.expirableAcquire(15_000, 10_000)) {
                fail("Can't acquire the second lock in the second time");
            }
            long acqDuration2 = System.currentTimeMillis() - acqStartTimeSnapshot;
            logger.info("The second acquire for lock A is {} ms", acqDuration2);
            assertThat(acqDuration2, lessThan(10_000L));

            backgroundExecutor.shutdown();
            if (!backgroundExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
                backgroundExecutor.shutdownNow();
            }
        }
    }


}