package ru.fix.distributed.job.manager;

import ru.fix.distributed.job.manager.model.JobDescriptor;

public interface WorkShareLockService extends AutoCloseable {

    boolean tryAcquire(
            JobDescriptor job,
            String workItem,
            WorkShareLockServiceImpl.LockProlongationFailedListener listener);

    boolean existsLock(JobDescriptor job, String workItem);

    void release(JobDescriptor job, String workItem);
}
