package ru.fix.distributed.job.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.dynamic.property.api.DynamicProperty;
import ru.fix.stdlib.concurrency.threads.Schedule;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

class StubbedMultiJob implements DistributedJob {

    private static final Logger logger = LoggerFactory.getLogger(StubbedMultiJob.class);
    private static final String DISTRIBUTED_JOB_ID_PATTERN = "distr-job-id-%d";

    private final int jobId;
    private Set<String> workPool;
    private final long delay;
    private final boolean singleThread;
    private final long workPoolExpirationPeriod;

    private AtomicReference<Set<String>> localWorkPool = new AtomicReference<>();
    private Set<Set<String>> allWorkPools = Collections.synchronizedSet(new HashSet<>());

    public StubbedMultiJob(int jobId, Set<String> workPool) {
        this(jobId, workPool, 100);
    }

    public StubbedMultiJob(int jobId, Set<String> workPool, long delay) {
        this(jobId, workPool, delay, true);
    }

    public StubbedMultiJob(int jobId, Set<String> workPool, long delay, long workPoolExpirationPeriod) {
        this(jobId, workPool, delay, workPoolExpirationPeriod, true);
    }

    public StubbedMultiJob(int jobId, Set<String> workPool, long delay, boolean singleThread) {
        this(jobId, workPool, delay, 0, singleThread);
    }

    public StubbedMultiJob(int jobId, Set<String> workPool, long delay, long workPoolExpirationPeriod, boolean
            singleThread) {
        this.jobId = jobId;
        this.workPool = workPool;
        this.delay = delay;
        this.workPoolExpirationPeriod = workPoolExpirationPeriod;
        this.singleThread = singleThread;
    }

    public void updateWorkPool(Set<String> newWorkPool) {
        workPool = newWorkPool;
    }

    @Override
    public WorkPool getWorkPool() {
        return WorkPool.of(workPool);
    }

    @Override
    public WorkPoolRunningStrategy getWorkPoolRunningStrategy() {
        return singleThread ? WorkPoolRunningStrategies.getSingleThreadStrategy()
                : WorkPoolRunningStrategies.getThreadPerWorkItemStrategy();
    }

    @Override
    public long getInitialJobDelay() {
        return 0;
    }

    @Override
    public DynamicProperty<Schedule> getSchedule() {
        return Schedule.withDelay(DynamicProperty.of(delay));
    }

    @Override
    public long getWorkPoolCheckPeriod() {
        return workPoolExpirationPeriod;
    }

    @Override
    public String getJobId() {
        return getJobId(jobId);
    }

    @Override
    public void run(DistributedJobContext context) {
        logger.trace("{} Run distributed test job {} / {}", this, getJobId(), context.getWorkShare());
        localWorkPool.set(context.getWorkShare());
        allWorkPools.add(context.getWorkShare());
    }

    public Set<String> getLocalWorkPool() {
        Set<String> wp = localWorkPool.get();
        return wp == null ? Collections.emptySet() : wp;
    }

    public Set<Set<String>> getAllWorkPools() {
        return allWorkPools;
    }

    public static String getJobId(int id) {
        return String.format(DISTRIBUTED_JOB_ID_PATTERN, id);
    }


}