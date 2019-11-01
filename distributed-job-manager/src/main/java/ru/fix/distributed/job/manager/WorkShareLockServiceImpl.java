package ru.fix.distributed.job.manager;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.aggregating.profiler.Profiler;
import ru.fix.dynamic.property.api.DynamicProperty;
import ru.fix.stdlib.concurrency.threads.NamedExecutors;
import ru.fix.stdlib.concurrency.threads.ReschedulableScheduler;
import ru.fix.stdlib.concurrency.threads.Schedule;
import ru.fix.zookeeper.lock.PersistentExpiringDistributedLock;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Acquires locks for job for workItems, prolongs them and releases.
 *
 * @author Kamil Asfandiyarov
 */
public class WorkShareLockServiceImpl implements AutoCloseable, WorkShareLockService {

    private static final Logger log = LoggerFactory.getLogger(WorkShareLockServiceImpl.class);

    public static final long DEFAULT_RESERVATION_PERIOD_MS = TimeUnit.MINUTES.toMillis(15);
    public static final long DEFAULT_ACQUIRING_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(10);
    public static final long DEFAULT_EXPIRATION_PERIOD_MS = TimeUnit.MINUTES.toMillis(6);
    public static final long DEFAULT_LOCK_PROLONGATION_INTERVAL_MS = TimeUnit.MINUTES.toMillis(3);


    final CuratorFramework curatorFramework;
    final JobManagerPaths jobManagerPaths;
    final String workerId;
    final ExecutorService persistenLockExecutor;

    final ReschedulableScheduler workItemProlongationTask;

    @FunctionalInterface
    public interface LockProlongationFailedListener {
        void onLockProlongationFailed();
    }

    static class WorkItemLock {
        public final PersistentExpiringDistributedLock lock;
        public final LockProlongationFailedListener prolongationListener;

        public WorkItemLock(PersistentExpiringDistributedLock lock, LockProlongationFailedListener
                prolongationListener) {
            this.lock = lock;
            this.prolongationListener = prolongationListener;
        }

        public boolean release() {
            return this.lock.release();
        }

        public void close() throws Exception {
            lock.close();
        }
    }

    final Map<DistributedJob, Map<String, WorkItemLock>> jobWorkItemLocks = new ConcurrentHashMap<>();

    public WorkShareLockServiceImpl(CuratorFramework curatorFramework,
                                    JobManagerPaths jobManagerPaths,
                                    String workerId,
                                    Profiler profiler) {

        workItemProlongationTask = NamedExecutors.newSingleThreadScheduler(
                "worker-work-item-prolongation", profiler);
        persistenLockExecutor = NamedExecutors.newSingleThreadPool(
                "djm-work-item-persistent-lock-executor", profiler);
        this.curatorFramework = curatorFramework;
        this.jobManagerPaths = jobManagerPaths;
        this.workerId = workerId;

        this.workItemProlongationTask.schedule(
                DynamicProperty.of(Schedule.withDelay(DEFAULT_LOCK_PROLONGATION_INTERVAL_MS)),
                0,
                () -> jobWorkItemLocks.forEach((job, workItemLocks) ->
                        workItemLocks.forEach((workItem, lock) -> {
                                    if (!checkAndProlong(job, workItem, lock.lock)) {
                                        log.info("wid={} prolongation fail jobId={} item={}",
                                                workerId,
                                                job.getJobId(),
                                                workItem);
                                        lock.prolongationListener.onLockProlongationFailed();
                                    }
                                }
                        )));
    }

    @Override
    public boolean tryAcquire(DistributedJob job, String workItem, LockProlongationFailedListener listener) {
        Map<String, WorkItemLock> jobLocks = jobWorkItemLocks
                .computeIfAbsent(job, jobKey -> new ConcurrentHashMap<>());

        PersistentExpiringDistributedLock lock;
        try {
            String lockPath = jobManagerPaths.getWorkItemLock(job.getJobId(), workItem);
            lock = new PersistentExpiringDistributedLock(
                    curatorFramework,
                    persistenLockExecutor,
                    workerId,
                    lockPath,
                    workerId);
            if (!lock.expirableAcquire(DEFAULT_RESERVATION_PERIOD_MS, DEFAULT_ACQUIRING_TIMEOUT_MS)) {
                log.debug("Failed to acquire expirable lock. Acquire period: {}, timeout: {}, lock path: {}",
                        DEFAULT_RESERVATION_PERIOD_MS, DEFAULT_ACQUIRING_TIMEOUT_MS, lockPath);
                lock.close();
                return false;
            }

            WorkItemLock oldLock = jobLocks.put(workItem, new WorkItemLock(lock, listener));
            if (oldLock != null) {
                log.error("Illegal state of work item lock for job:{} workItem:{}. Lock already acquired.",
                        job.getJobId(), workItem);

                jobLocks.remove(workItem);

                oldLock.close();
                lock.close();
                return false;
            }
            log.info("wid={} acqired jobId={} item={}",
                    workerId,
                    job.getJobId(),
                    workItem);
            return true;

        } catch (Exception exc) {
            log.error("Failed to create PersistentExpiringDistributedLock for job {} for workItem {}",
                    job.getJobId(), workItem, exc);

            return false;
        }
    }

    @Override
    public boolean existsLock(DistributedJob job, String workItem) {
        Map<String, WorkItemLock> workItemLocks = this.jobWorkItemLocks.get(job);
        return workItemLocks != null && workItemLocks.containsKey(workItem);
    }

    @Override
    public void release(DistributedJob job, String workItem) {
        Map<String, WorkItemLock> workItemLocks = this.jobWorkItemLocks.get(job);
        if (workItemLocks == null) {
            log.error("Illegal state of work item lock for job:{} workItem:{}. Job workItem locks do not exist.",
                    job.getJobId(), workItem);
            return;
        }

        WorkItemLock lock = workItemLocks.get(workItem);
        if (lock == null) {
            log.error("Illegal state of work item lock for job:{} workItem:{}. Lock does not exist.",
                    job.getJobId(), workItem);
            return;
        }

        try {
            lock.release();
            log.info("wid={} released jobId={} item={}",
                    workerId,
                    job.getJobId(),
                    workItem);
            workItemLocks.remove(workItem);
        } finally {
            try {
                lock.close();
            } catch (Exception e) {
                log.error("error closing lock", e);
            }
        }
    }

    private static boolean checkAndProlong(DistributedJob job,
                                           String workItem,
                                           PersistentExpiringDistributedLock lock) {
        try {
            log.info("checkAndProlong jobId={} item={}",
                    job.getJobId(),
                    workItem);

            return lock.checkAndProlongIfExpiresIn(DEFAULT_RESERVATION_PERIOD_MS, DEFAULT_EXPIRATION_PERIOD_MS);
        } catch (Exception exc) {
            log.error("Failed to checkAndProlong persistent locks for job {} for workItem {}",
                    job.getJobId(), workItem, exc);
            return false;
        }
    }

    @Override
    public void close() {

        jobWorkItemLocks.forEach((job, workItemLocks) ->
                workItemLocks.forEach((workItem, lock) -> {
                            if (lock != null) {
                                try {
                                    lock.close();
                                } catch (Exception exc) {
                                    log.error("Failed to close lock for job {} for workItem {}",
                                            job.getJobId(), workItem, exc);
                                }
                            }
                        }
                ));
        jobWorkItemLocks.clear();

        persistenLockExecutor.shutdown();
    }
}
