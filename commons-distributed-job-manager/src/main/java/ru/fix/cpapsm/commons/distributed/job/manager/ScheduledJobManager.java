package ru.fix.cpapsm.commons.distributed.job.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

/**
 * Manage jobs that currently scheduled. Some of them are running and some will be launched any time.
 *
 * Implementation remark: DJM is not designed for frequent rebalance and scheduling.
 * We can work with {@code scheduledJobs} under common lock.
 *
 * @author eyakovleva
 */
class ScheduledJobManager {

    private static final Logger log = LoggerFactory.getLogger(ScheduledJobManager.class);

    //Jobs that currently scheduled. Some of them are running and some will be launched any time.
    private final Map<DistributedJob, List<ScheduledJobExecution>> scheduledJobs = new ConcurrentHashMap<>();

    synchronized List<ScheduledJobExecution> getScheduledJobExecutions(DistributedJob distributedJob) {
        return scheduledJobs.get(distributedJob);
    }

    synchronized void removeIf(Predicate<Map.Entry<DistributedJob, List<ScheduledJobExecution>>> predicate) {
        scheduledJobs.entrySet().removeIf(predicate);
    }

    synchronized void add(DistributedJob distributedJob, ScheduledJobExecution scheduledJobExecution) {
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

}
