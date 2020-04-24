package ru.fix.distributed.job.manager;

import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.aggregating.profiler.Profiler;
import ru.fix.distributed.job.manager.model.DistributedJobManagerSettings;

import ru.fix.distributed.job.manager.model.DistributedJobSettings;
import ru.fix.distributed.job.manager.util.WorkPoolUtils;
import ru.fix.distributed.job.manager.util.ZkTreePrinter;
import ru.fix.dynamic.property.api.AtomicProperty;
import ru.fix.dynamic.property.api.DynamicProperty;
import ru.fix.stdlib.concurrency.threads.NamedExecutors;
import ru.fix.stdlib.concurrency.threads.ReschedulableScheduler;
import ru.fix.stdlib.concurrency.threads.Schedule;
import ru.fix.zookeeper.transactional.TransactionalClient;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
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
    private static final Logger log = LoggerFactory.getLogger(Worker.class);
    private static final int WORKER_REGISTRATION_RETRIES_COUNT = 10;
    private static final int WORK_POOL_UPDATE_RETRIES_COUNT = 5;

    public static final String THREAD_NAME_DJM_WORKER_NONE = "djm-worker-none";

    private final CuratorFramework curatorFramework;

    private final Collection<DistributedJob> availableJobs;
    private final ScheduledJobManager scheduledJobManager = new ScheduledJobManager();

    private final ZkPathsManager paths;
    private final String workerId;
    private volatile TreeCache workPooledCache;

    private final ReschedulableScheduler jobReschedulableScheduler;

    private final ExecutorService assignmentUpdatesExecutor;
    private final ReschedulableScheduler workPoolReschedulableScheduler;
    private final Profiler profiler;

    private DynamicProperty<Map<String,Boolean>> jobSettings;
    private Supplier<Map<String,Boolean>> jobSettingsSupplier;
    private DynamicProperty<Long> timeToWaitTermination;

    private volatile boolean isWorkerShutdown = false;
    /**
     * Acquires locks for job for workItems, prolongs them and releases
     */
    private final WorkShareLockService workShareLockService;

    private final AtomicProperty<Integer> threadPoolSize;

    Worker(CuratorFramework curatorFramework,
           Collection<DistributedJob> distributedJobs,
           Profiler profiler,
           DistributedJobManagerSettings settings) {

        this.curatorFramework = curatorFramework;
        this.paths = new ZkPathsManager(settings.getRootPath());
        this.workerId = settings.getNodeId();
        this.availableJobs = distributedJobs;

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

        this.jobReschedulableScheduler = new ReschedulableScheduler(
                THREAD_NAME_DJM_WORKER_NONE,
                threadPoolSize,
                profiler);
        this.timeToWaitTermination = settings.getJobSettings().map(DistributedJobSettings::getTimeToWaitTermination);
        this.jobSettingsSupplier = () -> settings.getJobSettings().get().getJobsEnabledStatus();
        this.jobSettings = DynamicProperty.delegated(jobSettingsSupplier);

        this.workShareLockService = new WorkShareLockServiceImpl(
                curatorFramework,
                paths,
                workerId,
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
        registerWorkerAndJobs(workPools);

        availableJobs.forEach(v -> {
            long workPoolCheckPeriod = v.getWorkPoolCheckPeriod();
            if (workPoolCheckPeriod != 0) {
                workPoolReschedulableScheduler.schedule(
                        DynamicProperty.delegated(() -> Schedule.withDelay(v.getWorkPoolCheckPeriod())),
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

        TransactionalClient.tryCommit(
                curatorFramework,
                WORKER_REGISTRATION_RETRIES_COUNT,
                transaction -> {

                    int workPoolVersion = checkAndUpdateVersion(paths.availableWorkPoolVersion(), transaction);

                    int workerVersion = checkAndUpdateVersion(paths.workerVersion(), transaction);

                    log.info("Registering worker {} (worker version {}, work-pool version {})",
                            workerId, workerVersion, workPoolVersion);

                    registerWorkerAsAlive(transaction);

                    recreateWorkerTree(transaction);

                    updateAvailableWorkPool(workPools, transaction);
                }
        );

        addListenerToAssignedTree();
    }

    private void closeListenerToAssignedTree() {
        if (workPooledCache != null) {
            try {
                workPooledCache.close();
            } catch (Exception e) {
                log.error("Failed to close pooled assignment listener", e);
            }
        }
    }

    /**
     * @param path        node, which version should be checked and updated
     * @param transaction transaction, which used to check the version of node
     * @return previous version of updating node
     */
    private int checkAndUpdateVersion(String path, TransactionalClient transaction) throws Exception {
        int version = curatorFramework.checkExists().forPath(path).getVersion();
        transaction.checkPathWithVersion(path, version);
        transaction.setData(path, new byte[]{});
        return version;
    }

    private void registerWorkerAsAlive(TransactionalClient transaction) throws Exception {
        String nodeAlivePath = paths.aliveWorker(workerId);
        if (curatorFramework.checkExists().forPath(nodeAlivePath) != null) {
            transaction.deletePath(nodeAlivePath);
        }
        transaction.createPathWithMode(nodeAlivePath, CreateMode.EPHEMERAL);
    }

    private void recreateWorkerTree(TransactionalClient transaction) throws Exception {
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
            transaction.createPath(paths.availableJob(workerId, job.getJobId()));
        }
    }

    private void updateAvailableWorkPool(
            ConcurrentMap<DistributedJob, WorkPool> workPools, TransactionalClient transaction) throws Exception {

        for (DistributedJob job : availableJobs) {
            String workPoolsPath = paths.availableWorkPool(job.getJobId());
            if (curatorFramework.checkExists().forPath(workPoolsPath) == null) {
                transaction.createPath(workPoolsPath);
            }
            Set<String> newWorkPool = workPools.get(job).getItems();
            Set<String> currentWorkPool = getChildrenIfNodeExists(workPoolsPath);
            updateZkJobWorkPool(newWorkPool, currentWorkPool, job.getJobId(), transaction);
        }
    }

    private void addListenerToAssignedTree() throws Exception {
        workPooledCache = new TreeCache(curatorFramework, paths.assignedJobs(workerId));
        workPooledCache.getListenable().addListener((client, event) -> {
            log.info("wid={} registerWorkerAsAliveAndRegisterJobs event={}", workerId, event);
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


    private boolean updateZkJobWorkPool(
            Set<String> newWorkPool, Set<String> currentWorkPool, String jobId, TransactionalClient transaction
    ) throws Exception {
        if (!currentWorkPool.equals(newWorkPool)) {
            log.info("wid={} updateZkJobWorkPool update for jobId={} from {} to {}",
                    workerId,
                    jobId,
                    currentWorkPool,
                    newWorkPool);

            createItemsContainedInFirstSetButNotInSecond(newWorkPool, currentWorkPool, transaction, jobId);
            removeItemsContainedInFirstSetButNotInSecond(currentWorkPool, newWorkPool, transaction, jobId);

            return true;
        } else {
            log.info("wid={} updateZkJobWorkPool update unneed for jobId={} pool={}",
                    workerId,
                    jobId,
                    newWorkPool);
            return false;
        }
    }

    private void createItemsContainedInFirstSetButNotInSecond(
            Set<String> newWorkPool, Set<String> currentWorkPool, TransactionalClient transaction, String jobId
    ) throws Exception {
        Set<String> workPoolsToAdd = new HashSet<>(newWorkPool);
        workPoolsToAdd.removeAll(currentWorkPool);
        for (String workItemToAdd : workPoolsToAdd) {
            transaction.createPath(paths.availableWorkItem(jobId, workItemToAdd));
        }
    }

    private void removeItemsContainedInFirstSetButNotInSecond(
            Set<String> currentWorkPool, Set<String> newWorkPool, TransactionalClient transaction, String jobId
    ) throws Exception {
        Set<String> workPoolsToDelete = new HashSet<>(currentWorkPool);
        workPoolsToDelete.removeAll(newWorkPool);
        for (String workItemToDelete : workPoolsToDelete) {
            transaction.deletePath(paths.availableWorkItem(jobId, workItemToDelete));
        }
    }

    private void updateWorkPoolForJob(DistributedJob job, Set<String> newWorkPool) {
        try {
            String jobId = job.getJobId();
            String workPoolsPath = paths.availableWorkPool(jobId);
            String availableJobPath = paths.availableJob(workerId, jobId);
            String workerAliveFlagPath = paths.aliveWorker(workerId);

            TransactionalClient.tryCommit(
                    curatorFramework,
                    WORK_POOL_UPDATE_RETRIES_COUNT,
                    transaction -> {

                        checkAndUpdateVersion(paths.availableWorkPoolVersion(), transaction);

                        boolean workPoolsPathExists = null != transaction.checkPath(workPoolsPath);
                        boolean availableJobPathExists = null != transaction.checkPath(availableJobPath);
                        boolean workerAliveFlagPathExists = null != transaction.checkPath(workerAliveFlagPath);

                        if (workPoolsPathExists && availableJobPathExists && workerAliveFlagPathExists) {
                            Set<String> currentWorkPools = getChildrenIfNodeExists(workPoolsPath);
                            boolean workPoolUpdated
                                    = updateZkJobWorkPool(newWorkPool, currentWorkPools, jobId, transaction);

                            if (workPoolUpdated) {
                                transaction.setData(paths.aliveWorker(workerId),
                                        curatorFramework.getData().forPath(workerAliveFlagPath));
                            }
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

    private Set<String> getChildrenIfNodeExists(String path) throws Exception {
        if (curatorFramework.checkExists().forPath(path) == null) {
            return Collections.emptySet();
        } else {
            return new HashSet<>(curatorFramework.getChildren().forPath(path));
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
        //supplying status of the job based on job's id to pass it to ScheduledJobExecution
        Supplier<Boolean> supplyStatusOfJob = () -> jobSettings.get().get(newMultiJob.getJobId());
        log.info("wid={} onWorkPooledJobReassigned start jobId={} with {}, delay={}, and isEnabled={}",
                workerId,
                newMultiJob.getJobId(),
                workPoolToExecute,
                newMultiJob.getInitialJobDelay(),
                DynamicProperty.delegated(supplyStatusOfJob));

        ScheduledJobExecution jobExecutionWrapper = new ScheduledJobExecution(
                newMultiJob,
                new HashSet<>(workPoolToExecute),
                profiler,
                new SmartLockMonitorDecorator(workShareLockService),
                DynamicProperty.delegated(supplyStatusOfJob)
        );

        if (!isWorkerShutdown) {
            ScheduledFuture<?> scheduledFuture =
                    jobReschedulableScheduler.schedule(
                            newMultiJob.getSchedule(),
                            newMultiJob.getInitialJobDelay(),
                            jobExecutionWrapper);
            jobExecutionWrapper.setScheduledFuture(scheduledFuture);
            scheduledJobManager.add(newMultiJob, jobExecutionWrapper);

            log.debug("Future {} with hash={} scheduled for jobId={} with {} and delay={}",
                    scheduledFuture, System.identityHashCode(scheduledFuture),
                    newMultiJob.getJobId(), workPoolToExecute, newMultiJob.getInitialJobDelay());
        } else {
            log.warn("Cannot schedule wid={} jobId={} with {} and delay={}. Worker is in shutdown state",
                    workerId, newMultiJob.getJobId(), workPoolToExecute, newMultiJob.getInitialJobDelay());
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
                    .forPath(paths.assignedWorkPool(workerId, job.getJobId()));
        } catch (KeeperException.NoNodeException e) {
            log.trace("Received event when NoNode for work pool path {}", e, e);
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
            String nodeAlivePath = paths.aliveWorker(workerId);
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
