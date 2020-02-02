package ru.fix.distributed.job.manager;

import org.apache.curator.utils.ZKPaths;

class JobManagerPaths {
    public static final String ALIVE = "alive";
    public static final String ASSIGNMENT_VERSION = "assignment-version";
    public static final String LEADER_LATCH = "leader-latch";
    public static final String LOCKS = "locks";
    public static final String REGISTRATION_VERSION = "registration-version";
    public static final String WORKERS = "workers";
    public static final String ASSIGNED = "assigned";
    public static final String AVAILABLE = "available";
    public static final String WORK_POOLED_JOB_ID = "work-pooled";
    public static final String WORK_POOL = "work-pool";

    final String rootPath;

    JobManagerPaths(String rootPath) {
        this.rootPath = rootPath;
    }

    public String toAliveWorkers() {
        return ZKPaths.makePath(rootPath, ALIVE);
    }

    String toAliveWorker(String workerId) {
        return ZKPaths.makePath(rootPath, ALIVE, workerId);
    }

    String toAssignmentVersion() {
        return ZKPaths.makePath(rootPath, ASSIGNMENT_VERSION);
    }

    String toLeaderLatch() {
        return ZKPaths.makePath(rootPath, LEADER_LATCH);
    }

    String toLocks() {
        return ZKPaths.makePath(rootPath, LOCKS);
    }

    String toWorkItemLock(String jobId, String workItem) {
        return ZKPaths.makePath(rootPath, LOCKS, jobId, String.format("work-share-%s.lock", workItem));
    }

    String toRegistrationVersion() {
        return ZKPaths.makePath(rootPath, REGISTRATION_VERSION);
    }

    String toAllWorkers() {
        return ZKPaths.makePath(rootPath, WORKERS);
    }

    String toWorker(String workerId) {
        return ZKPaths.makePath(rootPath, WORKERS, workerId);
    }

    String toAssignedWorkPool(String workerId) {
        return ZKPaths.makePath(rootPath, WORKERS, workerId, ASSIGNED);
    }

    String toAssignedJobs(String workerId) {
        return ZKPaths.makePath(rootPath, WORKERS, workerId, ASSIGNED, WORK_POOLED_JOB_ID);
    }

    String toAssignedWorkPool(String workerId, String jobId) {
        return ZKPaths.makePath(rootPath, WORKERS, workerId, ASSIGNED, WORK_POOLED_JOB_ID, jobId);
    }

    String toAssignedWorkItems(String workerId, String jobId) {
        return ZKPaths.makePath(rootPath, WORKERS, workerId, ASSIGNED, WORK_POOLED_JOB_ID, jobId, WORK_POOL);
    }

    String toAssignedWorkItem(String workerId, String jobId, String workItemId) {
        return ZKPaths.makePath(rootPath, WORKERS, workerId, ASSIGNED, WORK_POOLED_JOB_ID, jobId, WORK_POOL, workItemId);
    }

    String toAvailableWorkPool(String workerId) {
        return ZKPaths.makePath(rootPath, WORKERS, workerId, AVAILABLE);
    }

    String toAvailableJobs(String workerId) {
        return ZKPaths.makePath(rootPath, WORKERS, workerId, AVAILABLE, WORK_POOLED_JOB_ID);
    }

    String toAvailableWorkItems(String workerId, String jobId) {
        return ZKPaths.makePath(rootPath, WORKERS, workerId, AVAILABLE, WORK_POOLED_JOB_ID, jobId, WORK_POOL);
    }

    String toAvailableWorkItem(String workerId, String jobId, String workItemId) {
        return ZKPaths.makePath(rootPath, WORKERS, workerId, AVAILABLE, WORK_POOLED_JOB_ID, jobId, WORK_POOL, workItemId);
    }

}
