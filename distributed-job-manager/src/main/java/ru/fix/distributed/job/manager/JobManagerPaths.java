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

    public String getWorkersAlivePath() {
        return ZKPaths.makePath(rootPath, ALIVE);
    }

    String getWorkerAliveFlagPath(String workerId) {
        return ZKPaths.makePath(rootPath, ALIVE, workerId);
    }

    String getAssignmentVersion() {
        return ZKPaths.makePath(rootPath, ASSIGNMENT_VERSION);
    }

    String getLeaderLatchPath() {
        return ZKPaths.makePath(rootPath, LEADER_LATCH);
    }

    String getWorkPooledLocksPath() {
        return ZKPaths.makePath(rootPath, LOCKS, WORK_POOLED_JOB_ID);
    }

    String getWorkItemLock(String jobId, String workItem) {
        return ZKPaths.makePath(rootPath, LOCKS, WORK_POOLED_JOB_ID, jobId,
                String.format("work-share-%s.lock", workItem));
    }

    String getRegistrationVersion() {
        return ZKPaths.makePath(rootPath, REGISTRATION_VERSION);
    }

    String getWorkersPath() {
        return ZKPaths.makePath(rootPath, WORKERS);
    }

    String getWorkerPath(String workerId) {
        return ZKPaths.makePath(rootPath, WORKERS, workerId);
    }

    String getWorkerAssignedJobsPath(String workerId) {
        return ZKPaths.makePath(rootPath, WORKERS, workerId, ASSIGNED);
    }

    String getAssignedWorkPooledJobsPath(String workerId) {
        return ZKPaths.makePath(rootPath, WORKERS, workerId, ASSIGNED, WORK_POOLED_JOB_ID);
    }

    String getAssignedWorkPooledJobsPath(String workerId, String jobId) {
        return ZKPaths.makePath(rootPath, WORKERS, workerId, ASSIGNED, WORK_POOLED_JOB_ID, jobId);
    }

    String getAssignedWorkPoolPath(String workerId, String jobId) {
        return ZKPaths.makePath(rootPath, WORKERS, workerId, ASSIGNED, WORK_POOLED_JOB_ID, jobId, WORK_POOL);
    }

    String getAssignedWorkItemPath(String workerId, String jobId, String workItemId) {
        return ZKPaths.makePath(rootPath, WORKERS, workerId, ASSIGNED, WORK_POOLED_JOB_ID, jobId, WORK_POOL, workItemId);
    }

    String getWorkerAvailableJobsPath(String workerId) {
        return ZKPaths.makePath(rootPath, WORKERS, workerId, AVAILABLE);
    }

    String getAvailableWorkPooledJobPath(String workerId) {
        return ZKPaths.makePath(rootPath, WORKERS, workerId, AVAILABLE, WORK_POOLED_JOB_ID);
    }

    String getAvailableWorkPoolPath(String workerId, String jobId) {
        return ZKPaths.makePath(rootPath, WORKERS, workerId, AVAILABLE, WORK_POOLED_JOB_ID, jobId, WORK_POOL);
    }

    String getAvailableWorkItemPath(String workerId, String jobId, String workItemId) {
        return ZKPaths.makePath(rootPath, WORKERS, workerId, AVAILABLE, WORK_POOLED_JOB_ID, jobId, WORK_POOL, workItemId);
    }

}
