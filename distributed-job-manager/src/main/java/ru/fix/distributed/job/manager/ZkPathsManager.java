package ru.fix.distributed.job.manager;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;

public class ZkPathsManager {
    private static final String ALIVE = "alive";
    private static final String LEADER_LATCH = "leader-latch";
    private static final String LOCKS = "locks";
    private static final String WORKER_ASSIGNMENT_VERSION = "worker-assignment-version";
    private static final String WORKER_VERSION = "worker-version";
    private static final String WORKERS = "workers";
    private static final String ASSIGNED = "assigned";
    private static final String AVAILABLE = "available";
    private static final String WORK_POOL = "work-pool";
    private static final String WORK_POOL_VERSION = "work-pool-version";

    final String rootPath;

    public ZkPathsManager(String rootPath) {
        this.rootPath = rootPath;
    }

    public void initPaths(CuratorFramework curatorFramework) throws Exception {
        createIfNeeded(curatorFramework, this.allWorkers());
        createIfNeeded(curatorFramework, this.aliveWorkers());
        createIfNeeded(curatorFramework, this.workerVersion());
        createIfNeeded(curatorFramework, this.assignmentVersion());
        createIfNeeded(curatorFramework, this.locks());
        createIfNeeded(curatorFramework, this.availableWorkPool());
        createIfNeeded(curatorFramework, this.availableWorkPoolVersion());
    }

    private static void createIfNeeded(CuratorFramework curatorFramework, String path) throws Exception {
        if (curatorFramework.checkExists().forPath(path) == null) {
            curatorFramework.create().creatingParentsIfNeeded().forPath(path);
        }
    }

    public String aliveWorkers() {
        return path(ALIVE);
    }

    public String aliveWorker(String workerId) {
        return path(ALIVE, workerId);
    }

    public String assignmentVersion() {
        return path(WORKER_ASSIGNMENT_VERSION);
    }

    public String leaderLatch() {
        return path(LEADER_LATCH);
    }

    public String locks() {
        return path(LOCKS);
    }

    public String workItemLock(String jobId, String workItem) {
        return path(LOCKS, jobId, String.format("work-share-%s.lock", workItem));
    }

    public String jobWorkItemLocks(String jobId) {
        return path(LOCKS, jobId);
    }

    public String workerVersion() {
        return path(WORKER_VERSION);
    }

    public String allWorkers() {
        return path(WORKERS);
    }

    public String worker(String workerId) {
        return path(WORKERS, workerId);
    }

    public String assignedJobs(String workerId) {
        return path(WORKERS, workerId, ASSIGNED);
    }

    public String assignedWorkPool(String workerId, String jobId) {
        return path(WORKERS, workerId, ASSIGNED, jobId);
    }

    public String assignedWorkItem(String workerId, String jobId, String workItemId) {
        return path(WORKERS, workerId, ASSIGNED, jobId, workItemId);
    }

    public String availableJobs(String workerId) {
        return path(WORKERS, workerId, AVAILABLE);
    }

    public String availableJob(String workerId, String jobId) {
        return path(WORKERS, workerId, AVAILABLE, jobId);
    }

    public String availableWorkPool() {
        return path(WORK_POOL);
    }

    public String availableWorkPool(String jobId) {
        return path(WORK_POOL, jobId);
    }

    public String availableWorkItem(String jobId, String workItemId) {
        return path(WORK_POOL, jobId, workItemId);
    }

    public String availableWorkPoolVersion() {
        return path(WORK_POOL_VERSION);
    }

    private String path(String firstChild) {
        return ZKPaths.makePath(rootPath, firstChild);
    }

    private String path(String firstChild, String... restChildren) {
        return ZKPaths.makePath(rootPath, firstChild, restChildren);
    }
}
