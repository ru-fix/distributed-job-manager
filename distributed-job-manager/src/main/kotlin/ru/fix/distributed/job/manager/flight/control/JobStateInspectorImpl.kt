package ru.fix.distributed.job.manager.flight.control

import org.apache.curator.framework.CuratorFramework
import ru.fix.distributed.job.manager.ZkPathsManager
import java.lang.IllegalArgumentException

class JobStateInspectorImpl(
    zkRootPaths: Set<String>,
    val curatorFramework: CuratorFramework
) : JobStateInspector {

    val zkPathsManagers = zkRootPaths.map { ZkPathsManager(it) }

    override fun getJobStatus(jobId: String): JobStatus {
        val availableJobPathsManager = zkPathsManagers.filter {
            val availableJobsPath = it.availableWorkPool(jobId)
            curatorFramework.checkExists().forPath(availableJobsPath) != null
        }

        if (availableJobPathsManager.isEmpty()) {
            throw IllegalArgumentException("Job with id=$jobId is not available")
        }

        val jobLocksPath = availableJobPathsManager.single().jobLock(jobId)

        val jobLockNode = curatorFramework.checkExists().forPath(jobLocksPath)
        return if (jobLockNode == null || jobLockNode.numChildren == 0) {
            JobStatus.STOPPED
        } else {
            JobStatus.STARTED
        }
    }
}