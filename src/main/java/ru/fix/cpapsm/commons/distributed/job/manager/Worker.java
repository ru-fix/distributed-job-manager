package ru.fix.cpapsm.commons.distributed.job.manager;

import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.aggregating.profiler.Profiler;
import ru.fix.commons.zookeeper.transactional.TransactionalClient;
import ru.fix.cpapsm.commons.distributed.job.manager.util.WorkPoolUtils;
import ru.fix.cpapsm.commons.distributed.job.manager.util.ZkTreePrinter;
import ru.fix.dynamic.property.api.AtomicProperty;
import ru.fix.dynamic.property.api.DynamicProperty;
import ru.fix.stdlib.concurrency.threads.NamedExecutors;
import ru.fix.stdlib.concurrency.threads.ProfiledScheduledThreadPoolExecutor;
import ru.fix.stdlib.concurrency.threads.ReschedulableScheduler;
import ru.fix.stdlib.concurrency.threads.Schedule;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Launches jobs, listen to changes in assignment section of zookeeper tree made by Master and starts / stops jobs
 * accordingly.
 *
 * @author Kamil Asfandiyarov
 * @see JobManagerPaths
 * @see Manager
 */
class Worker implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Worker.class);

    private static final int REGISTRATION_COMMIT_RETRIES_COUNT = 10;
    private static final int WORK_POOL_RETRIES_COUNT = 5;
    public static final String THREAD_NAME_DJM_WORKER_NONE = "djm-worker-none";

    private final CuratorFramework curatorFramework;

    private final Collection<DistributedJob> availableJobs;
    private final DynamicProperty<Boolean> printTree;
    private final ScheduledJobManager scheduledJobManager = new ScheduledJobManager();

    private final JobManagerPaths paths;
    private final String workerId;
    private volatile TreeCache workPooledCache;

    private final ReschedulableScheduler jobReschedulableScheduler;

    private final ExecutorService assignmentUpdatesExecutor;
    private final ReschedulableScheduler workPoolReschedulableScheduler;
    private final Profiler profiler;

    private DynamicProperty<Long> timeToWaitTermination;
    private final String serverId;

    private volatile boolean isWorkerShutdown = false;
    /**
     * Acquires locks for job for workItems, prolongs them and releases
     */
    final WorkShareLockService workShareLockService;

    private final AtomicProperty<Integer> threadPoolSize;

    Worker(CuratorFramework curatorFramework,
           String workerId,
           String rootPath,
           Collection<DistributedJob> distributedJobs,
           Profiler profiler,
           DynamicProperty<Long> timeToWaitTermination,
           String serverId,
           DynamicProperty<Boolean> printTree) {
        this.curatorFramework = curatorFramework;
        this.paths = new JobManagerPaths(rootPath);
        this.workerId = workerId;
        this.availableJobs = distributedJobs;
        this.printTree = printTree;

        this.assignmentUpdatesExecutor = NamedExecutors.newSingleThreadPool(
                "worker-" + workerId,
                profiler);
        this.profiler = profiler;
        this.workPoolReschedulableScheduler = NamedExecutors.newScheduler(
                "worker-update-thread",
                DynamicProperty.of(distributedJobs.size()),
                profiler
        );

        threadPoolSize = new AtomicProperty<>(1);

        ProfiledScheduledThreadPoolExecutor jobExecutor = NamedExecutors.newScheduledExecutor(
                THREAD_NAME_DJM_WORKER_NONE,
                threadPoolSize,
                profiler
        );

        this.jobReschedulableScheduler = new ReschedulableScheduler(jobExecutor);
        this.timeToWaitTermination = timeToWaitTermination;
        this.serverId = serverId;

        this.workShareLockService = new WorkShareLockServiceImpl(
                curatorFramework,
                paths,
                workerId,
                serverId,
                profiler);

        distributedJobs.forEach(job ->
                profiler.attachIndicator(ProfilerMetrics.RUN_INDICATOR(job.getJobId()), () -> {
                    List<ScheduledJobExecution> executions = scheduledJobManager.getScheduledJobExecutions(job);
                    if (executions != null) {
                        return executions.stream()
                                .mapToLong(ScheduledJobExecution::getRunningJobsCount)
                                .sum();
                    } else {
                        return 0L;
                    }
                })
        );
    }

    public void start() throws Exception {
        ConcurrentMap<DistributedJob, WorkPool> workPools = availableJobs.stream()
                .collect(Collectors.toConcurrentMap(k -> k, DistributedJob::getWorkPool));
        workPools.forEach(WorkPoolUtils::checkWorkPoolItemsRestrictions);
        registerWorkerAsAliveAndRegisterJobs(workPools);

        availableJobs.forEach(v -> {
            long workPoolCheckPeriod = v.getWorkPoolCheckPeriod();
            if (workPoolCheckPeriod != 0) {
                workPoolReschedulableScheduler.schedule(
                        () -> Schedule.withDelay(v.getWorkPoolCheckPeriod()),
                        workPoolCheckPeriod,
                        () -> {
                            if (isWorkerShutdown) {
                                return;
                            }
                            WorkPool workPool = v.getWorkPool();
                            WorkPoolUtils.checkWorkPoolItemsRestrictions(v, workPool);
                            updateWorkPoolForJob(v, workPool.getItems());
                        }
                );
            }
        });

        curatorFramework.getConnectionStateListenable().addListener((client, event) -> {
            log.info("sid={} start().connectionListener event={}", serverId, event);
            if (!isWorkerShutdown) {
                switch (event) {
                    case SUSPENDED:
                        assignmentUpdatesExecutor.submit(this::shutdownAllJobExecutions);
                        break;
                    case RECONNECTED:
                        assignmentUpdatesExecutor.submit(() -> {
                            try {
                                ConcurrentMap<DistributedJob, WorkPool> workPoolsMap = availableJobs.stream()
                                        .collect(Collectors.toConcurrentMap(k -> k, DistributedJob::getWorkPool));
                                workPoolsMap.forEach(WorkPoolUtils::checkWorkPoolItemsRestrictions);
                                registerWorkerAsAliveAndRegisterJobs(workPoolsMap);
                            } catch (Exception e) {
                                log.error("Failed to register available jobs", e);
                            }
                        });
                        break;
                }
            }
        });
    }

    private void registerWorkerAsAliveAndRegisterJobs(
            ConcurrentMap<DistributedJob, WorkPool> workPools) throws Exception {
        // close listeners
        if (workPooledCache != null) {
            try {
                workPooledCache.close();
            } catch (Exception e) {
                log.error("Failed to close pooled assignment listener", e);
            }
        }

        TransactionalClient.tryCommit(curatorFramework, REGISTRATION_COMMIT_RETRIES_COUNT, availableJobsTransaction -> {
            // check node version
            String checkNodePath = paths.getRegistrationVersion();
            int version = curatorFramework.checkExists().forPath(checkNodePath).getVersion();

            availableJobsTransaction.checkPathWithVersion(checkNodePath, version);
            availableJobsTransaction.setData(checkNodePath, new byte[]{});

            log.info("Registering worker {} (registration version {})", workerId, version);

            // register worker as alive
            String nodeAlivePath = paths.getWorkerAliveFlagPath(workerId);
            if (curatorFramework.checkExists().forPath(nodeAlivePath) != null) {
                availableJobsTransaction.deletePath(nodeAlivePath);
            }
            availableJobsTransaction.createPathWithMode(nodeAlivePath, CreateMode.EPHEMERAL);

            // clean up all available jobs
            String workerPath = paths.getWorkerPath(workerId);
            if (curatorFramework.checkExists().forPath(workerPath) != null) {
                availableJobsTransaction.deletePathWithChildrenIfNeeded(workerPath);
            }

            // create availability and assignments directories
            availableJobsTransaction.createPath(paths.getWorkerPath(workerId));
            availableJobsTransaction.createPath(paths.getWorkerAvailableJobsPath(workerId));
            availableJobsTransaction.createPath(paths.getAvailableWorkPooledJobPath(workerId));
            availableJobsTransaction.createPath(paths.getWorkerAssignedJobsPath(workerId));
            availableJobsTransaction.createPath(paths.getAssignedWorkPooledJobsPath(workerId));

            // register work pooled jobs
            for (DistributedJob job : availableJobs) {
                availableJobsTransaction.createPath(
                        ZKPaths.makePath(paths.getAvailableWorkPooledJobPath(workerId), job.getJobId()));
                availableJobsTransaction.createPath(paths.getAvailableWorkPoolPath(workerId, job.getJobId()));

                for (String workPool : workPools.get(job).getItems()) {
                    availableJobsTransaction.createPath(
                            ZKPaths.makePath(paths.getAvailableWorkPoolPath(workerId, job.getJobId()), workPool));
                }
            }
        });

        workPooledCache = new TreeCache(curatorFramework, paths.getAssignedWorkPooledJobsPath(workerId));
        workPooledCache.getListenable().addListener((client, event) -> {
            log.info("sid={} registerWorkerAsAliveAndRegisterJobs event={}", serverId, event);
            switch (event.getType()) {
                case INITIALIZED:
                case NODE_ADDED:
                case NODE_REMOVED:
                    if (!isWorkerShutdown) {
                        assignmentUpdatesExecutor.submit(() -> {
                            try {
                                onWorkPooledJobReassigned();
                            } catch (Exception e) {
                                log.error("Failed to perform work pool reassignment", e);
                            }
                        });
                    }
                    break;
            }
        });
        workPooledCache.start();
    }

    private void updateWorkPoolForJob(DistributedJob job, Set<String> newWorkPool) {
        try {
            String workPoolsPath = paths.getAvailableWorkPoolPath(workerId, job.getJobId());
            String workerAliveFlagPath = paths.getWorkerAliveFlagPath(workerId);

            boolean workPoolsPathExists = null != curatorFramework.checkExists().forPath(workPoolsPath);
            boolean workerAliveFlagPathExists = null != curatorFramework.checkExists().forPath(workerAliveFlagPath);

            if (workPoolsPathExists && workerAliveFlagPathExists) {
                Set<String> currentWorkPools = new HashSet<>(curatorFramework.getChildren().forPath(workPoolsPath));
                if (!currentWorkPools.equals(newWorkPool)) {
                    log.info("sid={} updateWorkPoolForJob update for jobId={} from {} to {}",
                            serverId,
                            job.getJobId(),
                            currentWorkPools,
                            newWorkPool);
                    TransactionalClient.tryCommit(curatorFramework, WORK_POOL_RETRIES_COUNT,
                            availableJobsTransaction -> {
                                // update alive flag to trigger update
                                availableJobsTransaction.setData(
                                        paths.getWorkerAliveFlagPath(workerId),
                                        curatorFramework.getData().forPath(workerAliveFlagPath));

                                // delete stale work pools
                                Set<String> workPoolsToDelete = new HashSet<>(currentWorkPools);
                                workPoolsToDelete.removeAll(newWorkPool);
                                for (String workPoolToDelete : workPoolsToDelete) {
                                    availableJobsTransaction.deletePath(ZKPaths.makePath(workPoolsPath,
                                            workPoolToDelete));
                                }

                                // create new work pools
                                Set<String> workPoolsToAdd = new HashSet<>(newWorkPool);
                                workPoolsToAdd.removeAll(currentWorkPools);
                                for (String workPoolToAdd : workPoolsToAdd) {
                                    availableJobsTransaction.createPath(ZKPaths.makePath(workPoolsPath, workPoolToAdd));
                                }
                            });
                } else {
                    log.info("sid={} updateWorkPoolForJob update unneed for jobId={} pool={}",
                            serverId,
                            job.getJobId(),
                            newWorkPool);
                }
            } else {
                log.warn("Received work pool update before worker registration," +
                                " job {} wp {}. State workPoolsPathExists {}, workerAliveFlagPathExists {}",
                        job, newWorkPool, workPoolsPathExists, workerAliveFlagPathExists);
            }
        } catch (Exception e) {
            log.error("Failed to update work pool for job {} wp {}", job, newWorkPool, e);
        }
    }

    private void reconfigureExecutors(Map<DistributedJob, Integer> workPooledMultiJobThreadCounts) {
        int threadsCount = workPooledMultiJobThreadCounts.values().stream().mapToInt(v -> v).sum();
        log.trace("Pool size now is {}", threadsCount);
        threadPoolSize.set(threadsCount);
    }

    private void onWorkPooledJobReassigned() throws Exception {
        log.trace("Invoke Worker#onWorkPooledJobReassigned() in {} worker", workerId);

        if (printTree.get() && log.isInfoEnabled()) {
            log.info("sid={} tree after reassign: \n {}", serverId, buildZkTreeDump());
        }

        // get new assignment
        Map<DistributedJob, Set<String>> newAssignments = getAssignedWorkPools(availableJobs);

        // configure executors
        Map<DistributedJob, Integer> threadCounts = getThreadCounts(newAssignments);
        reconfigureExecutors(threadCounts);

        // stop already running jobs which changed their states
        scheduledJobManager.removeIf(scheduledEntry -> {
            DistributedJob multiJob = scheduledEntry.getKey();
            List<ScheduledJobExecution> jobExecutions = scheduledEntry.getValue();

            // stop job work pools if they are not equal
            Set<String> newWorkPool = newAssignments.get(multiJob);
            Set<String> runningWorkPools = jobExecutions.stream()
                    .map(ScheduledJobExecution::getWorkShare)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet());
            if (!runningWorkPools.equals(newWorkPool)) {
                log.info("sid={} onWorkPooledJobReassigned reassigned jobId={} from {} to {}",
                        serverId,
                        multiJob.getJobId(),
                        runningWorkPools,
                        newWorkPool);
                safelyTerminateJobs(jobExecutions);
                //remove current entry from scheduledJobs
                return true;
            } else {
                // add logging to detect "job stop problem" https://jira.fix.ru/browse/CPAPSM-11106
                // should be not reachable since 11.0
                jobExecutions.forEach(scheduledJobExecution -> {
                    if (scheduledJobExecution.isShutdowned()) {
                        log.error("sid={} onWorkPooledJobReassigned jobId={} is down! Work share {}",
                                serverId,
                                multiJob.getJobId(),
                                scheduledJobExecution.getWorkShare());
                    }
                });
            }
            return false;
        });

        // start required jobs
        for (Map.Entry<DistributedJob, Set<String>> newAssignment : newAssignments.entrySet()) {
            DistributedJob newMultiJob = newAssignment.getKey();
            List<String> newWorkPool = new ArrayList<>(newAssignment.getValue());
            if (scheduledJobManager.getScheduledJobExecutions(newMultiJob) == null && !newWorkPool.isEmpty()) {
                int threadCount = threadCounts.get(newMultiJob);
                int groupsSize = newWorkPool.size() / threadCount;

                for (List<String> workPoolToExecute : Lists.partition(newWorkPool, groupsSize)) {
                    log.info("sid={} onWorkPooledJobReassigned start jobId={} with {} and delay={}",
                            serverId,
                            newMultiJob.getJobId(),
                            workPoolToExecute,
                            newMultiJob.getInitialJobDelay());
                    ScheduledJobExecution jobExecutionWrapper = new ScheduledJobExecution(
                            newMultiJob,
                            new HashSet<>(workPoolToExecute),
                            profiler,
                            new SmartLockMonitorDecorator(workShareLockService)
                    );

                    if (!isWorkerShutdown) {

                        ScheduledFuture<?> scheduledFuture =
                                jobReschedulableScheduler.schedule(
                                        newMultiJob::getSchedule,
                                        newMultiJob.getInitialJobDelay(),
                                        jobExecutionWrapper);
                        jobExecutionWrapper.setScheduledFuture(scheduledFuture);
                        scheduledJobManager.add(newMultiJob, jobExecutionWrapper);
                    } else {
                        log.warn("Cannot schedule sid={} jobId={} with {} and delay={}. Worker is in shutdown state",
                                serverId, newMultiJob.getJobId(), workPoolToExecute, newMultiJob.getInitialJobDelay());
                    }
                }
            }
        }
    }

    private Map<DistributedJob, Set<String>> getAssignedWorkPools(
            Collection<DistributedJob> workPooledMultiJobs) throws Exception {
        Map<DistributedJob, Set<String>> newAssignments = new HashMap<>();
        for (DistributedJob workPooledMultiJob : workPooledMultiJobs) {
            newAssignments.put(workPooledMultiJob, new HashSet<>(getWorkerWorkPool(workPooledMultiJob)));
        }
        return newAssignments;
    }

    private List<String> getWorkerWorkPool(DistributedJob job) throws Exception {
        try {
            return curatorFramework.getChildren().forPath(paths.getAssignedWorkPoolPath(workerId, job.getJobId()));
        } catch (KeeperException.NoNodeException e) {
            log.trace("Received event when NoNode for work pool path {}", e);
            return Collections.emptyList();
        }
    }

    private static Map<DistributedJob, Integer> getThreadCounts(
            Map<DistributedJob, Set<String>> assignedWorkPools) {
        return assignedWorkPools.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        o -> o.getKey().getWorkPoolRunningStrategy().getThreadCount(o.getValue()))
                );
    }

    public void safelyTerminateJobs(Collection<ScheduledJobExecution> jobExecutions) {
        jobExecutions.forEach(ScheduledJobExecution::shutdown);

        long totalTime = timeToWaitTermination.get();
        long startTime = System.currentTimeMillis();
        for (ScheduledJobExecution jobExecutionWrapper : jobExecutions) {
            log.info("sid={} safelyTerminateJobs shutdown {}",
                    serverId,
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

    private String buildZkTreeDump() {
        try {
            return new ZkTreePrinter(curatorFramework).print(paths.rootPath);
        } catch (Exception ex) {
            log.warn("Failed to build zk tree", ex);
            return "";
        }
    }

    @Override
    public void close() throws Exception {
        isWorkerShutdown = true;
        long closingStart = System.currentTimeMillis();

        // shutdown cache to stop updates
        workPooledCache.close();

        // shutdown work pool update executor
        workPoolReschedulableScheduler.shutdown();

        // shutdown assignment update executor
        assignmentUpdatesExecutor.shutdown();

        // shutdown contexts for running jobs
        shutdownAllJobExecutions();

        // shutdown jobs executor
        jobReschedulableScheduler.shutdown();

        // await all executors completion
        long timeToWait = timeToWaitTermination.get();

        long executorPoolTerminationTime = awaitAndTerminate(jobReschedulableScheduler, timeToWait);
        timeToWait -= executorPoolTerminationTime;
        log.info("Job executor pool is terminated. Awaiting time: {} ms.", executorPoolTerminationTime);

        long workPoolExecutorTerminationTime = awaitAndTerminate(workPoolReschedulableScheduler, timeToWait);
        timeToWait -= workPoolExecutorTerminationTime;
        log.info("Work pool update executor service is terminated. Awaiting time: {} ms.",
                workPoolExecutorTerminationTime);

        long assignmentExecutorTerminationTime = awaitAndTerminate(assignmentUpdatesExecutor, timeToWait);
        log.info("Assignment update executor service is terminated. Awaiting time: {} ms.",
                assignmentExecutorTerminationTime);


        // remove node alive flag
        try {
            String nodeAlivePath = paths.getWorkerAliveFlagPath(workerId);
            if (curatorFramework.checkExists().forPath(nodeAlivePath) != null) {
                curatorFramework.delete().forPath(nodeAlivePath);
            }
        } catch (Exception e) {
            log.warn("Alive node was already terminated", e);
        }

        workShareLockService.close();


        log.info("Distributed job manager closing completed. Closing took {} ms.",
                System.currentTimeMillis() - closingStart);
    }

    /**
     * Awaits executor termination timeToWait milliseconds and calls {@link ExecutorService#shutdownNow()} if executor
     * still isn't completed
     *
     * @param timeToWait time to wait, in milliseconds. Could be 0 or less than 0, that means, that
     *                   {@link ExecutorService#shutdownNow()} will be performed immediately
     * @return time, which spend for termination await
     */
    private static long awaitAndTerminate(ExecutorService executorService, long timeToWait) throws
            InterruptedException {
        long startTime = System.currentTimeMillis();
        if (timeToWait < 0) {
            executorService.shutdownNow();
        } else if (!executorService.awaitTermination(timeToWait, TimeUnit.MILLISECONDS)) {
            log.error("Failed to wait worker thread pool termination. Force shutdownNow.");
            executorService.shutdownNow();
        }
        return System.currentTimeMillis() - startTime;
    }

    private static long awaitAndTerminate(ReschedulableScheduler reschedulableScheduler, long timeToWait)
            throws InterruptedException {
        long startTime = System.currentTimeMillis();
        if (timeToWait < 0) {
            reschedulableScheduler.shutdownNow();
        } else if (!reschedulableScheduler.awaitTermination(timeToWait, TimeUnit.MILLISECONDS)) {
            log.error("Failed to wait worker thread pool termination. Force shutdownNow.");
            reschedulableScheduler.shutdownNow();
        }
        return System.currentTimeMillis() - startTime;
    }

    private void shutdownAllJobExecutions() {
        scheduledJobManager.shutdownAllJobExecutions();
    }
}
