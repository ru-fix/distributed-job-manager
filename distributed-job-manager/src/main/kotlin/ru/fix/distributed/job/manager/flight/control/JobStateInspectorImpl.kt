package ru.fix.distributed.job.manager.flight.control

import org.apache.curator.framework.CuratorFramework
import ru.fix.distributed.job.manager.ZkPathsManager
import java.lang.IllegalArgumentException

class JobStateInspectorImpl(
    zkRootPath: String,
    val curatorFramework: CuratorFramework
) : JobStateInspector {

    val zkPathsManager = ZkPathsManager(zkRootPath)

    override fun getJobStatus(jobId: String): JobStatus {
        val jobLocksPath = zkPathsManager.jobLock(jobId)
        val availableJobsPath = zkPathsManager.availableWorkPool(jobId)

        if (curatorFramework.checkExists().forPath(availableJobsPath) == null) {
            throw IllegalArgumentException("Job with id=$jobId is not available")
        }

        val jobLockNode = curatorFramework.checkExists().forPath(jobLocksPath)
        return if (jobLockNode == null || jobLockNode.numChildren == 0) {
            JobStatus.STOPPED
        } else {
            JobStatus.STARTED
        }
    }
}