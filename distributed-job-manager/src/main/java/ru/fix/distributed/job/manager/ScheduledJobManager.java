package ru.fix.distributed.job.manager;

import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.aggregating.profiler.Profiler;
import ru.fix.distributed.job.manager.model.DistributedJobManagerSettings;
import ru.fix.distributed.job.manager.model.JobDescriptor;
import ru.fix.dynamic.property.api.AtomicProperty;
import ru.fix.dynamic.property.api.DynamicProperty;
import ru.fix.stdlib.concurrency.threads.ReschedulableScheduler;
import ru.fix.zookeeper.lock.PersistentExpiringLockManager;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;


/**
 * Manage jobs that currently scheduled. Some of them are running and some will be launched any time.
 * <p>
 * Implementation remark: DJM is not designed for frequent rebalance and scheduling.
 * We can work with {@code scheduledJobs} under common lock.
 *
 * @author eyakovleva
 */
class ScheduledJobManager {

    private static final Logger log = LoggerFactory.getLogger(ScheduledJobManager.class);

    private final String workerId;

    private final Profiler profiler;

    private final ZkPathsManager paths;

    private final AtomicBoolean isWorkerShutdown;
    private final DynamicProperty<Long> timeToWaitTermination;

    private final AtomicProperty<Integer> threadPoolSize;
    private final ReschedulableScheduler jobReschedulableScheduler;

    /**
     * Acquires locks for job for workItems, prolongs them and releases
     */
    private final PersistentExpiringLockManager lockManager;

    //Jobs that currently scheduled. Some of them are running and some will be launched any time.
    private final Map<JobDescriptor, List<ScheduledJobExecution>> scheduledJobs = new ConcurrentHashMap<>();

    public ScheduledJobManager(
            CuratorFramework curatorFramework,
            ZkPathsManager paths,
            Profiler profiler,
            String workerId,
            DynamicProperty<DistributedJobManagerSettings> settings,
            AtomicBoolean isWorkerShutdown
    ) {
        this.workerId = workerId;
        this.profiler = profiler;
        this.paths = paths;
        this.isWorkerShutdown = isWorkerShutdown;
        this.timeToWaitTermination = settings.map(it -> it.getTimeToWaitTermination().toMillis());
        this.threadPoolSize = new AtomicProperty<>(1);
        this.jobReschedulableScheduler = new ReschedulableScheduler(
                "job-scheduler",
                threadPoolSize,
                profiler);
        this.lockManager = new PersistentExpiringLockManager(
                curatorFramework,
                settings.map(DistributedJobManagerSettings::getLockManagerConfig),
                profiler
        );
    }

    synchronized List<ScheduledJobExecution> getScheduledJobExecutions(JobDescriptor distributedJob) {
        return scheduledJobs.get(distributedJob);
    }

    private synchronized void removeIf(Predicate<Map.Entry<JobDescriptor, List<ScheduledJobExecution>>> predicate) {
        scheduledJobs.entrySet().removeIf(predicate);
    }

    private synchronized void add(JobDescriptor distributedJob, ScheduledJobExecution scheduledJobExecution) {
        scheduledJobs.computeIfAbsent(distributedJob, e -> Collections.synchronizedList(new ArrayList<>()))
                .add(scheduledJobExecution);
    }

    /**
     * Could be called not only on server shutdown but on zookeeper events. For example, on connection SUSPEND event.
     */
    synchronized void shutdownAllJobExecutions() {
        scheduledJobs.values().stream()
                .flatMap(Collection::stream)
                .forEach(scheduledExecution -> {
                    try {
                        scheduledExecution.shutdown();
                    } catch (Exception exc) {
                        log.error("Failed to shutdown execution jobId={}, workShare={}",
                                scheduledExecution.getJobId(),
                                scheduledExecution.getWorkShare(),
                                exc);
                    }
                });
        scheduledJobs.clear();
    }

    public void restartJobsAccordingToNewAssignments(Map<JobDescriptor, Set<String>> newAssignments) {

        // configure executors
        Map<JobDescriptor, Integer> threadCounts = getThreadCounts(newAssignments);
        reconfigureExecutors(threadCounts);

        // stop already running jobs which changed their states
        removeIf(scheduledEntry -> {
            JobDescriptor multiJob = scheduledEntry.getKey();
            List<ScheduledJobExecution> jobExecutions = scheduledEntry.getValue();

            // stop job work pools if they are not equal
            Set<String> newWorkPool = newAssignments.get(multiJob);
            Set<String> runningWorkPools = jobExecutions.stream()
                    .map(ScheduledJobExecution::getWorkShare)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet());
            if (!runningWorkPools.equals(newWorkPool)) {
                log.info("wid={} onWorkPooledJobReassigned reassigned jobId={} from {} to {}",
                        workerId,
                        multiJob.getJobId(),
                        runningWorkPools,
                        newWorkPool);
                safelyTerminateJobs(jobExecutions);
                //remove current entry from scheduledJobs
                return true;
            } else {
                jobExecutions.forEach(scheduledJobExecution -> {
                    if (scheduledJobExecution.isShutdowned()) {
                        log.error("wid={} onWorkPooledJobReassigned jobId={} is down! Work share {}",
                                workerId,
                                multiJob.getJobId(),
                                scheduledJobExecution.getWorkShare());
                    }
                });
            }
            return false;
        });

        // start required jobs
        for (Map.Entry<JobDescriptor, Set<String>> newAssignment : newAssignments.entrySet()) {
            JobDescriptor newMultiJob = newAssignment.getKey();
            List<String> newWorkPool = new ArrayList<>(newAssignment.getValue());
            if (getScheduledJobExecutions(newMultiJob) == null && !newWorkPool.isEmpty()) {
                int threadCount = threadCounts.get(newMultiJob);
                int groupsSize = newWorkPool.size() / threadCount;

                for (List<String> workPoolToExecute : Lists.partition(newWorkPool, groupsSize)) {
                    scheduleExecutingWorkPoolForJob(workPoolToExecute, newMultiJob);
                }
            }
        }
    }

    private void scheduleExecutingWorkPoolForJob(List<String> workPoolToExecute, JobDescriptor newMultiJob) {
        DynamicProperty<Long> initialJobDelay = newMultiJob.getInitialJobDelay();
        long initialJobDelayVal = initialJobDelay.get();
        log.info("wid={} onWorkPooledJobReassigned start jobId={} with {} and delay={}",
                workerId,
                newMultiJob.getJobId(),
                workPoolToExecute,
                initialJobDelayVal);
        ScheduledJobExecution jobExecutionWrapper = new ScheduledJobExecution(
                newMultiJob,
                new HashSet<>(workPoolToExecute),
                profiler,
                lockManager,
                paths
        );

        if (!isWorkerShutdown.get()) {
            ScheduledFuture<?> scheduledFuture =
                    jobReschedulableScheduler.schedule(
                            newMultiJob.getSchedule(),
                            initialJobDelay,
                            jobExecutionWrapper);
            jobExecutionWrapper.setScheduledFuture(scheduledFuture);
            add(newMultiJob, jobExecutionWrapper);

            log.debug("Future {} with hash={} scheduled for jobId={} with {} and delay={}",
                    scheduledFuture, System.identityHashCode(scheduledFuture),
                    newMultiJob.getJobId(), workPoolToExecute, initialJobDelayVal);
        } else {
            log.warn("Cannot schedule wid={} jobId={} with {} and delay={}. Worker is in shutdown state",
                    workerId, newMultiJob.getJobId(), workPoolToExecute, initialJobDelayVal);
        }
    }


    private void reconfigureExecutors(Map<JobDescriptor, Integer> workPooledMultiJobThreadCounts) {
        int threadsCount = workPooledMultiJobThreadCounts.values().stream().mapToInt(v -> v).sum();
        threadsCount = Math.max(threadsCount, 1);
        log.trace("Pool size now is {}", threadsCount);
        threadPoolSize.set(threadsCount);
    }

    private static Map<JobDescriptor, Integer> getThreadCounts(
            Map<JobDescriptor, Set<String>> assignedWorkPools) {
        return assignedWorkPools.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        o -> o.getKey().getWorkPoolRunningStrategy().getThreadCount(o.getValue()))
                );
    }

    private void safelyTerminateJobs(Collection<ScheduledJobExecution> jobExecutions) {
        jobExecutions.forEach(ScheduledJobExecution::shutdown);

        long totalTime = timeToWaitTermination.get();
        long startTime = System.currentTimeMillis();
        for (ScheduledJobExecution jobExecutionWrapper : jobExecutions) {
            log.info("wid={} safelyTerminateJobs shutdown {}",
                    workerId,
                    jobExecutionWrapper.getJobId());
            try {
                long spend = System.currentTimeMillis() - startTime;
                if (!jobExecutionWrapper.awaitTermination(Math.max(0, totalTime - spend), TimeUnit.MILLISECONDS)) {
                    log.error("Job {} is not completed in {} ms after request to shutdown",
                            jobExecutionWrapper.getJobId(),
                            System.currentTimeMillis() - startTime);
                }
            } catch (InterruptedException exc) {
                log.error("Interrupted job execution {}", jobExecutionWrapper.getJobId(), exc);
                Thread.currentThread().interrupt();
            }
        }
    }

    public void shutdown() {
        jobReschedulableScheduler.shutdown();
    }

    public long awaitAndTerminate(long timeToWait) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        if (timeToWait < 0) {
            jobReschedulableScheduler.shutdownNow();
        } else if (!jobReschedulableScheduler.awaitTermination(timeToWait, TimeUnit.MILLISECONDS)) {
            log.error("Failed to wait worker jobReschedulableScheduler termination. Force shutdownNow.");
            jobReschedulableScheduler.shutdownNow();
        }
        lockManager.close();
        return System.currentTimeMillis() - startTime;
    }


}
