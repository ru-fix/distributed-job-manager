package ru.fix.distributed.job.manager

import mu.KotlinLogging
import org.apache.curator.framework.CuratorFramework
import ru.fix.zookeeper.transactional.TransactionalClient
import java.util.concurrent.ConcurrentMap

internal class AvailableWorkPoolSubTree(
        private val curatorFramework: CuratorFramework,
        private val paths: ZkPathsManager
) {
    companion object {
        private val log = KotlinLogging.logger {}
    }

    fun checkAndUpdateVersion(transaction: TransactionalClient): Int =
            transaction.checkAndUpdateVersion(paths.availableWorkPoolVersion())

    fun pruneOutDatedJobs(transaction: TransactionalClient, actualJobs: Set<String>) {
        for (jobIdFromZk in currentJobsFromZk()) {
            if (!actualJobs.contains(jobIdFromZk)) {
                log.debug("cleanWorkPool removing {}", jobIdFromZk)
                transaction.deletePathWithChildrenIfNeeded(paths.availableWorkPool(jobIdFromZk))
            }
        }
    }

    private fun currentJobsFromZk(): MutableList<String> = curatorFramework.children.forPath(paths.availableWorkPool())

    fun updateAllJobs(transaction: TransactionalClient, newWorkPools: ConcurrentMap<DistributedJob, WorkPool>) {
        for (job in newWorkPools.keys) {
            val workPoolsPath: String = paths.availableWorkPool(job.getJobId())
            if (curatorFramework.checkExists().forPath(workPoolsPath) == null) {
                transaction.createPath(workPoolsPath)
            }
            val newWorkPool = newWorkPools[job]!!.items
            updateJob(transaction, job.getJobId(), newWorkPool)
        }
    }

    fun updateJob(transaction: TransactionalClient, jobId: String, newWorkPool: Set<String>): Boolean {
        val workPoolFromZk: Set<String> = currentJobWorkPoolFromZk(jobId)
        if (workPoolFromZk != newWorkPool) {
            createItemsContainedInFirstSetButNotInSecond(newWorkPool, workPoolFromZk, transaction, jobId)
            removeItemsContainedInFirstSetButNotInSecond(workPoolFromZk, newWorkPool, transaction, jobId)
            return true
        }
        return false
    }

    private fun currentJobWorkPoolFromZk(jobId: String): Set<String> {
        val availableWorkPool = paths.availableWorkPool(jobId)
        return if (curatorFramework.checkExists().forPath(availableWorkPool) == null) {
            emptySet()
        } else {
            HashSet(curatorFramework.children.forPath(availableWorkPool))
        }
    }

    private fun createItemsContainedInFirstSetButNotInSecond(
            newWorkPool: Set<String>, currentWorkPool: Set<String>, transaction: TransactionalClient, jobId: String
    ) {
        val workPoolsToAdd: MutableSet<String> = java.util.HashSet(newWorkPool)
        workPoolsToAdd.removeAll(currentWorkPool)
        for (workItemToAdd in workPoolsToAdd) {
            transaction.createPath(paths.availableWorkItem(jobId, workItemToAdd))
        }
    }

    private fun removeItemsContainedInFirstSetButNotInSecond(
            currentWorkPool: Set<String>, newWorkPool: Set<String>, transaction: TransactionalClient, jobId: String
    ) {
        val workPoolsToDelete: MutableSet<String> = java.util.HashSet(currentWorkPool)
        workPoolsToDelete.removeAll(newWorkPool)
        for (workItemToDelete in workPoolsToDelete) {
            transaction.deletePath(paths.availableWorkItem(jobId, workItemToDelete))
        }
    }

}