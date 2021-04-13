package ru.fix.distributed.job.manager.flight.control

import org.apache.curator.framework.CuratorFramework
import ru.fix.distributed.job.manager.ZkPathsManager

class JobStateInspector(
    zkRootPath: String,
    val curatorFramework: CuratorFramework
) {

    val zkPathsManager = ZkPathsManager(zkRootPath)

    fun getJobStatus(jobId: String): JobStatus {
        val availableJobPath = zkPathsManager.availableWorkPool(jobId)

        if (curatorFramework.checkExists().forPath(availableJobPath) == null) {
            throw IllegalArgumentException("Job with id=$jobId is not available")
        }

        val jobLocksPath = zkPathsManager.jobWorkItemLocks(jobId)
        val jobLockNode = curatorFramework.checkExists().forPath(jobLocksPath)

        return if (jobLockNode == null || jobLockNode.numChildren == 0) {
            JobStatus.STOPPED
        } else {
            JobStatus.RUNNING
        }
    }
}

enum class JobStatus {
    RUNNING,
    STOPPED
}