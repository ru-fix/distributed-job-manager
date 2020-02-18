package ru.fix.distributed.job.manager;

import org.apache.curator.utils.ZKPaths;

class ZkPathsManager {
    private static final String ALIVE = "alive";
    private static final String ASSIGNMENT_VERSION = "assignment-version";
    private static final String LEADER_LATCH = "leader-latch";
    private static final String LOCKS = "locks";
    private static final String REGISTRATION_VERSION = "registration-version";
    private static final String WORKERS = "workers";
    private static final String ASSIGNED = "assigned";
    private static final String AVAILABLE = "available";

    final String rootPath;

    ZkPathsManager(String rootPath) {
        this.rootPath = rootPath;
    }

    public String aliveWorkers() {
        return path(ALIVE);
    }

    String aliveWorker(String workerId) {
        return path(ALIVE, workerId);
    }

    String assignmentVersion() {
        return path(ASSIGNMENT_VERSION);
    }

    String leaderLatch() {
        return path(LEADER_LATCH);
    }

    String locks() {
        return path(LOCKS);
    }

    String workItemLock(String jobId, String workItem) {
        return path(LOCKS, jobId, String.format("work-share-%s.lock", workItem));
    }

    String registrationVersion() {
        return path(REGISTRATION_VERSION);
    }

    String allWorkers() {
        return path(WORKERS);
    }

    String worker(String workerId) {
        return path(WORKERS, workerId);
    }

    String assignedJobs(String workerId) {
        return path(WORKERS, workerId, ASSIGNED);
    }

    String assignedWorkPool(String workerId, String jobId) {
        return path(WORKERS, workerId, ASSIGNED, jobId);
    }

    String assignedWorkItem(String workerId, String jobId, String workItemId) {
        return path(WORKERS, workerId, ASSIGNED, jobId, workItemId);
    }

    String availableJobs(String workerId) {
        return path(WORKERS, workerId, AVAILABLE);
    }

    String availableWorkPool(String workerId, String jobId) {
        return path(WORKERS, workerId, AVAILABLE, jobId);
    }

    String availableWorkItem(String workerId, String jobId, String workItemId) {
        return path(WORKERS, workerId, AVAILABLE, jobId, workItemId);
    }

    private String path(String firstChild) {
        return ZKPaths.makePath(rootPath, firstChild);
    }
    private String path(String firstChild, String... restChildren) {
        return ZKPaths.makePath(rootPath, firstChild, restChildren);
    }
}
