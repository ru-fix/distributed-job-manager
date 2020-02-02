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
        return path(ALIVE);
    }

    String getWorkerAliveFlagPath(String workerId) {
        return path(ALIVE, workerId);
    }

    String getAssignmentVersion() {
        return path(ASSIGNMENT_VERSION);
    }

    String getLeaderLatchPath() {
        return path(LEADER_LATCH);
    }

    String getWorkPooledLocksPath() {
        return path(LOCKS, WORK_POOLED_JOB_ID);
    }

    String getWorkItemLock(String jobId, String workItem) {
        return path(LOCKS, WORK_POOLED_JOB_ID, jobId,
                String.format("work-share-%s.lock", workItem));
    }

    String getRegistrationVersion() {
        return path(REGISTRATION_VERSION);
    }

    String getWorkersPath() {
        return path(WORKERS);
    }

    String getWorkerPath(String workerId) {
        return path(WORKERS, workerId);
    }

    String getWorkerAssignedJobsPath(String workerId) {
        return path(WORKERS, workerId, ASSIGNED);
    }

    String getAssignedWorkPooledJobsPath(String workerId) {
        return path(WORKERS, workerId, ASSIGNED, WORK_POOLED_JOB_ID);
    }

    String getAssignedWorkPooledJobsPath(String workerId, String jobId) {
        return path(WORKERS, workerId, ASSIGNED, WORK_POOLED_JOB_ID, jobId);
    }

    String getAssignedWorkPoolPath(String workerId, String jobId) {
        return path(WORKERS, workerId, ASSIGNED, WORK_POOLED_JOB_ID, jobId, WORK_POOL);
    }

    String getAssignedWorkItemPath(String workerId, String jobId, String workItemId) {
        return path(WORKERS, workerId, ASSIGNED, WORK_POOLED_JOB_ID, jobId, WORK_POOL, workItemId);
    }

    String getWorkerAvailableJobsPath(String workerId) {
        return path(WORKERS, workerId, AVAILABLE);
    }

    String getAvailableWorkPooledJobPath(String workerId) {
        return path(WORKERS, workerId, AVAILABLE, WORK_POOLED_JOB_ID);
    }

    String getAvailableWorkPoolPath(String workerId, String jobId) {
        return path(WORKERS, workerId, AVAILABLE, WORK_POOLED_JOB_ID, jobId, WORK_POOL);
    }

    String getAvailableWorkItemPath(String workerId, String jobId, String workItemId) {
        return path(WORKERS, workerId, AVAILABLE, WORK_POOLED_JOB_ID, jobId, WORK_POOL, workItemId);
    }

    private String path(String firstChild) {
        return ZKPaths.makePath(rootPath, firstChild);
    }
    private String path(String firstChild, String... restChildren) {
        return ZKPaths.makePath(rootPath, firstChild, restChildren);
    }
}
