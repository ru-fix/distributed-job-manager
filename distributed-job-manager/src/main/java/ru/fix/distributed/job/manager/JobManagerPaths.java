package ru.fix.distributed.job.manager;

import org.apache.curator.utils.ZKPaths;

class JobManagerPaths {
    private static final String ALIVE = "alive";
    private static final String ASSIGNMENT_VERSION = "assignment-version";
    private static final String LEADER_LATCH = "leader-latch";
    private static final String LOCKS = "locks";
    private static final String REGISTRATION_VERSION = "registration-version";
    private static final String WORKERS = "workers";
    private static final String ASSIGNED = "assigned";
    private static final String AVAILABLE = "available";

    final String rootPath;

    JobManagerPaths(String rootPath) {
        this.rootPath = rootPath;
    }

    public String toAliveWorkers() {
        return path(ALIVE);
    }

    String toAliveWorker(String workerId) {
        return path(ALIVE, workerId);
    }

    String toAssignmentVersion() {
        return path(ASSIGNMENT_VERSION);
    }

    String toLeaderLatch() {
        return path(LEADER_LATCH);
    }

    String toLocks() {
        return path(LOCKS);
    }

    String toWorkItemLock(String jobId, String workItem) {
        return path(LOCKS, jobId, String.format("work-share-%s.lock", workItem));
    }

    String toRegistrationVersion() {
        return path(REGISTRATION_VERSION);
    }

    String toAllWorkers() {
        return path(WORKERS);
    }

    String toWorker(String workerId) {
        return path(WORKERS, workerId);
    }

    String toAssignedJobs(String workerId) {
        return path(WORKERS, workerId, ASSIGNED);
    }

    String toAssignedWorkItems(String workerId, String jobId) {
        return path(WORKERS, workerId, ASSIGNED, jobId);
    }

    String toAssignedWorkItem(String workerId, String jobId, String workItemId) {
        return path(WORKERS, workerId, ASSIGNED, jobId, workItemId);
    }

    String toAvailableJobs(String workerId) {
        return path(WORKERS, workerId, AVAILABLE);
    }

    String toAvailableWorkItems(String workerId, String jobId) {
        return path(WORKERS, workerId, AVAILABLE, jobId);
    }

    String toAvailableWorkItem(String workerId, String jobId, String workItemId) {
        return path(WORKERS, workerId, AVAILABLE, jobId, workItemId);
    }

    private String path(String firstChild) {
        return ZKPaths.makePath(rootPath, firstChild);
    }
    private String path(String firstChild, String... restChildren) {
        return ZKPaths.makePath(rootPath, firstChild, restChildren);
    }
}
