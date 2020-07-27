package ru.fix.distributed.job.manager;

import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.aggregating.profiler.Profiler;
import ru.fix.distributed.job.manager.model.DistributedJobManagerSettings;
import ru.fix.distributed.job.manager.model.JobDisableConfig;
import ru.fix.distributed.job.manager.util.WorkPoolUtils;
import ru.fix.dynamic.property.api.AtomicProperty;
import ru.fix.dynamic.property.api.DynamicProperty;
import ru.fix.stdlib.concurrency.threads.NamedExecutors;
import ru.fix.stdlib.concurrency.threads.ReschedulableScheduler;
import ru.fix.stdlib.concurrency.threads.Schedule;
import ru.fix.zookeeper.lock.PersistentExpiringLockManager;
import ru.fix.zookeeper.transactional.ZkTransaction;
import ru.fix.zookeeper.utils.ZkTreePrinter;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Launches jobs, listen to changes in assignment section of zookeeper tree made by Master and starts / stops jobs
 * accordingly.
 *
 * @author Kamil Asfandiyarov
 * @see ZkPathsManager
 * @see Manager
 */
class Worker implements AutoCloseable {
    public static final String THREAD_NAME_DJM_WORKER_NONE = "djm-worker-none";
    private static final Logger log = LoggerFactory.getLogger(Worker.class);
    private static final int WORKER_REGISTRATION_RETRIES_COUNT = 10;
    private static final int WORK_POOL_UPDATE_RETRIES_COUNT = 5;
    private final CuratorFramework curatorFramework;

    private final Collection<DistributedJob> availableJobs;
    private final ScheduledJobManager scheduledJobManager = new ScheduledJobManager();

    private final ZkPathsManager paths;
    private final String workerId;
    private final AvailableWorkPoolSubTree workPoolSubTree;
    private final ReschedulableScheduler jobReschedulableScheduler;
    private final ExecutorService shutdownListenersExecutor;
    private final ExecutorService assignmentUpdatesExecutor;
    private final ReschedulableScheduler workPoolReschedulableScheduler;
    private final Profiler profiler;
    private final DynamicProperty<Long> timeToWaitTermination;
    private final DynamicProperty<JobDisableConfig> jobDisableConfig;
    /**
     * Acquires locks for job for workItems, prolongs them and releases
     */
    private final PersistentExpiringLockManager lockManager;
    private final AtomicProperty<Integer> threadPoolSize;
    private volatile CuratorCache workPooledCache;
    private volatile boolean isWorkerShutdown = false;

    Worker(CuratorFramework curatorFramework,
           Collection<DistributedJob> jobs,
           Profiler profiler,
           DistributedJobManagerSettings settings) {
        this.curatorFramework = curatorFramework;
        this.paths = new ZkPathsManager(settings.getRootPath());
        this.workerId = settings.getNodeId();

        assertAllJobsHasUniqueJobId(jobs);
        this.availableJobs = jobs;

        this.workPoolSubTree = new AvailableWorkPoolSubTree(curatorFramework, paths);

        this.assignmentUpdatesExecutor = NamedExecutors.newSingleThreadPool(
                "worker-" + workerId,
                profiler);
        this.shutdownListenersExecutor = NamedExecutors.newDynamicPool(
                "shutdown-listeners",
                settings.getShutdownListenersThreadPoolSize(),
                profiler);
        this.profiler = profiler;
        this.workPoolReschedulableScheduler = NamedExecutors.newScheduler(
                "worker-update-thread",
                DynamicProperty.of(jobs.size()),
                profiler
        );

        threadPoolSize = new AtomicProperty<>(1);

        this.jobReschedulableScheduler = new ReschedulableScheduler(
                THREAD_NAME_DJM_WORKER_NONE,
                threadPoolSize,
                profiler);

        this.timeToWaitTermination = settings.getTimeToWaitTermination();
        this.jobDisableConfig = settings.getJobDisableConfig();

        this.lockManager = new PersistentExpiringLockManager(
                curatorFramework,
                settings.getLockManagerConfig(),
                profiler
        );

        attachProfilerIndicators();
    }

    private void assertAllJobsHasUniqueJobId(Collection<DistributedJob> jobs) {
        if (jobs.stream()
                .map(job -> job.getJobId())
                .collect(Collectors.toSet())
                .size() != jobs.size())
            throw new IllegalArgumentException(
                    "There are two or more job instances with same JobId: " +
                            jobs.stream()
                                    .map(job -> job.getJobId().getId())
                                    .collect(Collectors.joining()));
    }

    private static Map<DistributedJob, Integer> getThreadCounts(
            Map<DistributedJob, Set<String>> assignedWorkPools) {
        return assignedWorkPools.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        o -> o.getKey().getWorkPoolRunningStrategy().getThreadCount(o.getValue()))
                );
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

    private void attachProfilerIndicators() {
        availableJobs.forEach(job -> profiler.attachIndicator(ProfilerMetrics.RUN_INDICATOR(job.getJobId()), () -> {
            List<ScheduledJobExecution> executions = scheduledJobManager.getScheduledJobExecutions(job);
            if (executions != null) {
                return executions.stream()
                        .mapToLong(ScheduledJobExecution::getRunningJobsCount)
                        .sum();
            } else {
                return 0L;
            }
        }));
    }

    public void start() throws Exception {
        ConcurrentMap<DistributedJob, WorkPool> workPools = availableJobs.stream()
                .collect(Collectors.toConcurrentMap(k -> k, DistributedJob::getWorkPool));
        workPools.forEach(WorkPoolUtils::checkWorkPoolItemsRestrictions);
        registerWorkerAndJobs(workPools);

        availableJobs.forEach(job -> {
            long workPoolCheckPeriod = job.getWorkPoolCheckPeriod();
            if (workPoolCheckPeriod != 0) {
                workPoolReschedulableScheduler.schedule(
                        DynamicProperty.delegated(() -> Schedule.withDelay(job.getWorkPoolCheckPeriod())),
                        workPoolCheckPeriod,
                        () -> {
                            if (isWorkerShutdown) {
                                return;
                            }
                            WorkPool workPool = WorkPool.of(Collections.emptySet());
                            try {
                                workPool = job.getWorkPool();
                            } catch (Exception exception) {
                                log.error("Failed to access job WorkPool {}", job.getJobId(), exception);
                            }
                            WorkPoolUtils.checkWorkPoolItemsRestrictions(job, workPool);
                            updateWorkPoolForJob(job, workPool.getItems());
                        }
                );
            }
        });

        curatorFramework.getConnectionStateListenable().addListener((client, event) -> {
            log.info("wid={} start().connectionListener event={}", workerId, event);
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
                                registerWorkerAndJobs(workPoolsMap);
                            } catch (Exception e) {
                                log.error("Failed to register available jobs", e);
                            }
                        });
                        break;
                }
            }
        });
    }

    private void registerWorkerAndJobs(ConcurrentMap<DistributedJob, WorkPool> workPools) throws Exception {

        closeListenerToAssignedTree();

        ZkTransaction.tryCommit(
                curatorFramework,
                WORKER_REGISTRATION_RETRIES_COUNT,
                transaction -> {

                    int workPoolVersion = workPoolSubTree.checkAndUpdateVersion(transaction);

                    int workerVersion = transaction.checkAndUpdateVersion(paths.workerVersion());

                    log.info("Registering worker {} (worker version {}, work-pool version {})",
                            workerId, workerVersion, workPoolVersion);

                    registerWorkerAsAlive(transaction);

                    recreateWorkerTree(transaction);

                    workPoolSubTree.updateAllJobs(transaction, workPools);
                }
        );

        addListenerToAssignedTree();
    }

    private void closeListenerToAssignedTree() {
        CuratorCache cache = workPooledCache;
        if (cache != null) {
            try {
                cache.close();
            } catch (Exception e) {
                log.error("Failed to close pooled assignment listener", e);
            }
        }
    }

    private void registerWorkerAsAlive(ZkTransaction transaction) throws Exception {
        String nodeAlivePath = paths.aliveWorker(workerId);
        if (curatorFramework.checkExists().forPath(nodeAlivePath) != null) {
            transaction.deletePath(nodeAlivePath);
        }
        transaction.createPathWithMode(nodeAlivePath, CreateMode.EPHEMERAL);
    }

    private void recreateWorkerTree(ZkTransaction transaction) throws Exception {
        // clean up all available jobs
        String workerPath = paths.worker(workerId);
        if (curatorFramework.checkExists().forPath(workerPath) != null) {
            transaction.deletePathWithChildrenIfNeeded(workerPath);
        }

        // create availability and assignments directories
        transaction.createPath(paths.worker(workerId));
        transaction.createPath(paths.availableJobs(workerId));
        transaction.createPath(paths.assignedJobs(workerId));

        // register work pooled jobs
        for (DistributedJob job : availableJobs) {
            transaction.createPath(paths.availableJob(workerId, job.getJobId().getId()));
        }
    }

    private void addListenerToAssignedTree() throws Exception {
        workPooledCache = CuratorCache.build(curatorFramework, paths.assignedJobs(workerId));
        Semaphore cacheInitLocker = new Semaphore(0);
        CuratorCacheListener curatorCacheListener = new CuratorCacheListener() {
            @Override
            public void event(Type type, ChildData oldData, ChildData data) {
                log.info("wid={} registerWorkerAsAliveAndRegisterJobs eventType={}", workerId, type);
                switch (type) {
                    case NODE_CHANGED:
                    case NODE_CREATED:
                    case NODE_DELETED:
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
            }

            @Override
            public void initialized() {
                cacheInitLocker.release();
            }
        };
        workPooledCache.listenable().addListener(curatorCacheListener);
        workPooledCache.start();
        while (!cacheInitLocker.tryAcquire(5, TimeUnit.SECONDS)) {
            if (isWorkerShutdown) return;
        }
    }

    private void updateWorkPoolForJob(DistributedJob job, Set<String> newWorkPool) {
        try {
            String jobId = job.getJobId().getId();
            String workPoolsPath = paths.availableWorkPool(jobId);
            String availableJobPath = paths.availableJob(workerId, jobId);
            String workerAliveFlagPath = paths.aliveWorker(workerId);

            ZkTransaction.tryCommit(
                    curatorFramework,
                    WORK_POOL_UPDATE_RETRIES_COUNT,
                    transaction -> {

                        workPoolSubTree.checkAndUpdateVersion(transaction);

                        boolean workPoolsPathExists = null != transaction.checkPath(workPoolsPath);
                        boolean availableJobPathExists = null != transaction.checkPath(availableJobPath);
                        boolean workerAliveFlagPathExists = null != transaction.checkPath(workerAliveFlagPath);

                        if (workPoolsPathExists && availableJobPathExists && workerAliveFlagPathExists) {
                            workPoolSubTree.updateJob(transaction, jobId, newWorkPool);
                        } else {
                            log.warn("Received work pool update before worker registration, job {} wp {}. State " +
                                            "workPoolsPathExists {}, availableJobPathExists {}, workerAliveFlagPathExists {}",
                                    job, newWorkPool,
                                    workPoolsPathExists, availableJobPathExists, workerAliveFlagPathExists);
                        }
                    }
            );

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

        if (log.isTraceEnabled()) {
            log.trace("wid={} tree after reassign: \n {}", workerId, buildZkTreeDump());
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
        for (Map.Entry<DistributedJob, Set<String>> newAssignment : newAssignments.entrySet()) {
            DistributedJob newMultiJob = newAssignment.getKey();
            List<String> newWorkPool = new ArrayList<>(newAssignment.getValue());
            if (scheduledJobManager.getScheduledJobExecutions(newMultiJob) == null && !newWorkPool.isEmpty()) {
                int threadCount = threadCounts.get(newMultiJob);
                int groupsSize = newWorkPool.size() / threadCount;

                for (List<String> workPoolToExecute : Lists.partition(newWorkPool, groupsSize)) {
                    scheduleExecutingWorkPoolForJob(workPoolToExecute, newMultiJob);
                }
            }
        }
    }

    private void scheduleExecutingWorkPoolForJob(List<String> workPoolToExecute, DistributedJob newMultiJob) {
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
                shutdownListenersExecutor,
                profiler,
                lockManager,
                jobDisableConfig,
                paths
        );

        if (!isWorkerShutdown) {
            ScheduledFuture<?> scheduledFuture =
                    jobReschedulableScheduler.schedule(
                            newMultiJob.getSchedule(),
                            initialJobDelay,
                            jobExecutionWrapper);
            jobExecutionWrapper.setScheduledFuture(scheduledFuture);
            scheduledJobManager.add(newMultiJob, jobExecutionWrapper);

            log.debug("Future {} with hash={} scheduled for jobId={} with {} and delay={}",
                    scheduledFuture, System.identityHashCode(scheduledFuture),
                    newMultiJob.getJobId(), workPoolToExecute, initialJobDelayVal);
        } else {
            log.warn("Cannot schedule wid={} jobId={} with {} and delay={}. Worker is in shutdown state",
                    workerId, newMultiJob.getJobId(), workPoolToExecute, initialJobDelayVal);
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
            return curatorFramework.getChildren()
                    .forPath(paths.assignedWorkPool(workerId, job.getJobId().getId()));
        } catch (KeeperException.NoNodeException e) {
            log.trace("Received event when NoNode for work pool path {}", e, e);
            return Collections.emptyList();
        }
    }

    public void safelyTerminateJobs(Collection<ScheduledJobExecution> jobExecutions) {
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
        CuratorCache curatorCache = workPooledCache;
        if (curatorCache != null) {
            curatorCache.close();
        }

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
            String nodeAlivePath = paths.aliveWorker(workerId);
            if (curatorFramework.checkExists().forPath(nodeAlivePath) != null) {
                curatorFramework.delete().forPath(nodeAlivePath);
            }
        } catch (Exception e) {
            log.warn("Alive node was already terminated", e);
        }

        lockManager.close();

        detachProfilerIndicators();

        log.info("Distributed job manager closing completed. Closing took {} ms.",
                System.currentTimeMillis() - closingStart);
    }

    private void shutdownAllJobExecutions() {
        scheduledJobManager.shutdownAllJobExecutions();
    }

    private void detachProfilerIndicators() {
        availableJobs.forEach(job -> profiler.detachIndicator(ProfilerMetrics.RUN_INDICATOR(job.getJobId())));
    }
}
