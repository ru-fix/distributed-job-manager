package ru.fix.distributed.job.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.aggregating.profiler.ProfiledCall;
import ru.fix.aggregating.profiler.Profiler;
import ru.fix.distributed.job.manager.model.JobDescriptor;
import ru.fix.zookeeper.lock.LockIdentity;
import ru.fix.zookeeper.lock.PersistentExpiringLockManager;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Runnable task that scheduled by {@link Worker}
 * Lock workShare and run job
 *
 * @author Kamil Asfandiyarov
 */
class ScheduledJobExecution implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ScheduledJobExecution.class);
    final AtomicBoolean shutdownFlag = new AtomicBoolean(false);
    final PersistentExpiringLockManager lockManager;
    private final JobDescriptor job;
    private final Set<String> workShare;
    private final Profiler profiler;
    private final ZkPathsManager zkPathsManager;
    private final Lock lock = new ReentrantLock();
    ConcurrentHashMap.KeySetView<JobContext, Boolean> jobRuns = ConcurrentHashMap.newKeySet();
    private volatile ScheduledFuture<?> scheduledFuture;
    private volatile long lastShutdownTime;


    public ScheduledJobExecution(
            JobDescriptor job,
            Set<String> workShare,
            Profiler profiler,
            PersistentExpiringLockManager lockManager,
            ZkPathsManager zkPathsManager
    ) {
        if (workShare.isEmpty()) {
            throw new IllegalArgumentException(
                    "ScheduledJobExecution should receive at least single workItem in workShare");
        }

        this.job = job;
        this.workShare = workShare;
        this.profiler = profiler;
        this.lockManager = lockManager;
        this.zkPathsManager = zkPathsManager;
    }

    @Override
    public void run() {

        ProfiledCall jobLaunchProfiledCall = profiler.profiledCall(ProfilerMetrics.JOB(job.getJobId()));

        JobContext jobContext = new JobContext(job.getJobId(), workShare);
        jobRuns.add(jobContext);

        //add jobContext to jobRuns and then check shutdown flag so we would not miss shutdown event
        if (shutdownFlag.get()) {
            log.info("Job {} scheduled launch is canceled due to shutdown status of job context.", job.getJobId());
            return;
        }

        try {
            lock.lock();

            for (String workItem : workShare) {

                /*
                 * Server shutdown or restart launches reassign process on Master
                 * Master removes worker assigned zk nodes from Worker1, and add new zk nodes to Worker2
                 * Worker1 still holds locks for jobA and try to gracefully shutdown
                 * Worker2 sees new zk node
                 * Worker2 tries to acquire lock for jobA and fails because Worker1 still holds the lock.
                 */
                if (!lockManager.tryAcquire(
                        new LockIdentity(zkPathsManager.workItemLock(job.getJobId().getId(), workItem), null),
                        lockIdentity -> jobContext.shutdown())
                ) {
                    log.info("Failed to tryAcquire work share '{}' for job '{}'. Job launching will rescheduled.",
                            workItem, job.getJobId());
                    return;
                }
            }

            jobLaunchProfiledCall.profileThrowable(() -> job.run(jobContext));


        } catch (Exception exc) {
            log.error("Failure in job execution. Job {}, Class {}",
                    job.getJobId(),
                    job.getClass().getSimpleName(),
                    exc);
        } finally {
            lock.unlock();
            jobRuns.remove(jobContext);
            try {
                for (String workItem : workShare) {
                    String workItemPath = zkPathsManager.workItemLock(job.getJobId().getId(), workItem);
                    LockIdentity lockId = new LockIdentity(workItemPath, null);
                    if (lockManager.isLockManaged(lockId)) {
                        lockManager.release(lockId);
                    }
                }
            } catch (Exception exc) {
                log.error("Failure in job after-run block. Job {}, Class {}",
                        job.getJobId(),
                        job.getClass().getSimpleName(),
                        exc);
            }

            if (shutdownFlag.get() && lastShutdownTime > 0) {
                long runningAfterShutdownTime = System.currentTimeMillis() - lastShutdownTime;
                log.info("During DJM shutdown job {} took {} ms to complete.",
                        job.getJobId(),
                        runningAfterShutdownTime);
            }
        }
    }


    public JobId getJobId() {
        return job.getJobId();
    }

    public Set<String> getWorkShare() {
        return this.workShare;
    }

    public ScheduledJobExecution setScheduledFuture(ScheduledFuture<?> scheduledFuture) {
        this.scheduledFuture = scheduledFuture;
        return this;
    }

    public void shutdown() {
        lastShutdownTime = System.currentTimeMillis();
        this.shutdownFlag.set(true);
        jobRuns.forEach(JobContext::shutdown);
        scheduledFuture.cancel(false);

        log.debug("Future {} with hash={} cancelled for jobId={} with {}",
                scheduledFuture, System.identityHashCode(scheduledFuture), job.getJobId(), workShare);
    }

    boolean isShutdowned() {
        return shutdownFlag.get();
    }

    public boolean awaitTermination(long time, TimeUnit timeUnit) throws InterruptedException {
        return lock.tryLock(time, timeUnit);
    }

    /**
     * @return currently running jobs count associated with this scheduled job execution
     */
    public int getRunningJobsCount() {
        return jobRuns.size();
    }
}
