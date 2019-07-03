package ru.fix.cpapsm.commons.distributed.job.manager;

import org.apache.curator.utils.ZKPaths;

/**
 * Class keep information about zookeeper paths
 * <pre>
 * {@code
 *
 *  job-manager
 *    └ alive                               //alive servers, list contains alive nodes (workers) that could run jobs
 *      └ 20
 *      └ 3
 *      ...
 *    └ locks
 *      └ work-pooled
 *        └ async.report.building.job
 *          └ workItemA.lock                //list of job locks so only one node could run job with same work item
 *        └ elasticsearch.upload.job        //only one worker should handle work item at the same time
 *          └ workItemC.lock
 *         ...
 *    └ assignment-version
 *    └ registration-version
 *    └ leader-latch
 *      └ ...
 *    └ workers
 *      └ 20
 *       ...
 *      └ 3
 *        └ available
 *          └ work-pooled
 *            └ async.report.building.job   //list of available jobs that worker with id '3' could run
 *              └ work-pool
 *                └ workItemA               //Work pool for particular worker
 *                └ workItemB
 *            └ elasticsearch.upload.job
 *              └ work-pool
 *                └ workItemC
 *             ...
 *        └ assigned
 *          └ work-pooled                   //List of assigned jobs to worker '3'
 *            └ async.report.building.job
 *              └ work-pool
 *                └ workItemA
 *                └ workItemB
 *            └ elasticsearch.upload.job
 *              └ work-pool
 *                └ workItemC
 *             ...
 *
 * </pre>
 *
 * @author Kamil Asfandiyarov
 */
class JobManagerPaths {
    public static final String WORK_POOLED_JOB_ID = "work-pooled";
    public static final String WORK_POOL = "work-pool";
    public static final String REGISTRATION_VERSION = "registration-verison";
    public static final String ASSIGNMENT_VERSION = "assignment-version";

    final String rootPath;

    JobManagerPaths(String rootPath) {
        this.rootPath = rootPath;
    }

    String getAssignmentVersion() {
        return ZKPaths.makePath(rootPath, ASSIGNMENT_VERSION);
    }

    String getRegistrationVersion() {
        return ZKPaths.makePath(rootPath, REGISTRATION_VERSION);
    }

    String getWorkersPath() {
        return ZKPaths.makePath(rootPath, "workers");
    }

    String getWorkerAliveFlagPath(String workerId) {
        return ZKPaths.makePath(getWorkersAlivePath(), workerId);
    }

    String getWorkerPath(String workerId) {
        return ZKPaths.makePath(getWorkersPath(), workerId);
    }

    String getWorkerAvailableJobsPath(String worker) {
        return ZKPaths.makePath(getWorkerPath(worker), "available");
    }

    String getWorkerAssignedJobsPath(String worker) {
        return ZKPaths.makePath(getWorkerPath(worker), "assigned");
    }

    String getLeaderLatchPath() {
        return ZKPaths.makePath(rootPath, "leader-latch");
    }

    public String getWorkersAlivePath() {
        return ZKPaths.makePath(rootPath, "alive");
    }

    String getAvailableWorkPooledJobPath(String workerId) {
        return ZKPaths.makePath(getWorkerPath(workerId), "available", WORK_POOLED_JOB_ID);
    }

    String getAvailableWorkPoolPath(String workerId, String jobId) {
        return ZKPaths.makePath(getAvailableWorkPooledJobPath(workerId), jobId, WORK_POOL);
    }

    String getAssignedWorkPoolPath(String workerId, String jobId) {
        return ZKPaths.makePath(getAssignedWorkPooledJobsPath(workerId), jobId, WORK_POOL);
    }

    String getAssignedWorkPooledJobsPath(String workerId) {
        return ZKPaths.makePath(getWorkersPath(), workerId, "assigned", WORK_POOLED_JOB_ID);
    }

    String getAssignedWorkPooledJobsPath(String workerId, String jobId) {
        return ZKPaths.makePath(getAssignedWorkPooledJobsPath(workerId), jobId);
    }

    String getWorkPooledLocksPath() {
        return ZKPaths.makePath(rootPath, "locks", WORK_POOLED_JOB_ID);
    }

    String getWorkItemLock(String jobId, String workItem) {
        return ZKPaths.makePath(rootPath, "locks", WORK_POOLED_JOB_ID, jobId,
                String.format("work-share-%s.lock", workItem));
    }

}
