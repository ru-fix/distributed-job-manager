package ru.fix.cpapsm.commons.distributed.job.manager;

import lombok.extern.slf4j.Slf4j;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Мониторит насколько долго не получается захватить lock у джобы.
 * Если не получается достаточно долго - только тогда будет сыпать error'ами
 */
@Slf4j
public class SmartLockMonitorDecorator implements WorkShareLockService {

    private static final long DEFAULT_ALARM_INTERVAL = TimeUnit.MINUTES.toMillis(10);

    private final WorkShareLockService wrappedService;

    /**
     * timestamp, когда первый раз не смогли захватить lock
     */
    private final AtomicReference<Long> firstFailedAcquireLockDate = new AtomicReference<>();

    /**
     * Интервал времени, при котором считаем что нормальным, что мы не смогли получить Lock
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
            DistributedJob job,
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
    public boolean existsLock(DistributedJob job, String workItem) {
        return wrappedService.existsLock(job, workItem);
    }

    @Override
    public void release(DistributedJob job, String workItem) {
        wrappedService.release(job, workItem);
    }

    @Override
    public void close() throws Exception {
        wrappedService.close();
    }
}
