package ru.fix.distributed.job.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.distributed.job.manager.model.JobDescriptor;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Monitors how long it's impossible to take lock from Job
 * If this process is long enough, it will report an error
 */
public class SmartLockMonitorDecorator implements WorkShareLockService {

    private static final Logger log = LoggerFactory.getLogger(SmartLockMonitorDecorator.class);

    private static final long DEFAULT_ALARM_INTERVAL = TimeUnit.MINUTES.toMillis(10);

    private final WorkShareLockService wrappedService;

    /**
     * timestamp, when the first unsuccessful attempt to take lock
     */
    private final AtomicReference<Long> firstFailedAcquireLockDate = new AtomicReference<>();

    /**
     * time interval during which it's ok we can't get lock
     */
    private final long alarmInterval;

    public SmartLockMonitorDecorator(WorkShareLockService wrappedService) {
        this(wrappedService, DEFAULT_ALARM_INTERVAL);
    }

    public SmartLockMonitorDecorator(WorkShareLockService wrappedService, long alarmInterval) {
        log.debug("Smart lock Monitor has been created");
        this.wrappedService = wrappedService;
        this.alarmInterval = alarmInterval;
    }

    @Override
    public boolean tryAcquire(
            JobDescriptor job,
            String workItem,
            WorkShareLockServiceImpl.LockProlongationFailedListener listener
    ) {
        boolean lockIsTaken = wrappedService.tryAcquire(job, workItem, listener);

        if (!lockIsTaken) {
            // this is first time when can`t take a lock
            long now = System.currentTimeMillis();
            Long firstTs = firstFailedAcquireLockDate.updateAndGet(date -> date != null ? date : now);

            if (firstTs + alarmInterval < now) {
                log.error("Failed to acquire work share '{}' for job '{}' for a long time until '{}'",
                        workItem, job.getJobId(), new Date(firstTs));
            }

        } else { // yes, we can take a lock!
            firstFailedAcquireLockDate.set(null);
        }

        return lockIsTaken;
    }


    @Override
    public boolean existsLock(JobDescriptor job, String workItem) {
        return wrappedService.existsLock(job, workItem);
    }

    @Override
    public void release(JobDescriptor job, String workItem) {
        wrappedService.release(job, workItem);
    }

    @Override
    public void close() throws Exception {
        wrappedService.close();
    }
}
