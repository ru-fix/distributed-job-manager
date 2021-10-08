package ru.fix.distributed.job.manager

import org.apache.curator.framework.CuratorFramework
import org.apache.logging.log4j.kotlin.Logging
import ru.fix.distributed.job.manager.model.JobDescriptor
import ru.fix.zookeeper.transactional.ZkTransaction
import java.util.concurrent.ConcurrentMap

internal class AvailableWorkPoolSubTree(
    private val curatorFramework: CuratorFramework,
    private val paths: ZkPathsManager
) {
    companion object : Logging

    fun readVersionThenCheckAndUpdateIfTxMutatesState(transaction: ZkTransaction): Int =
        transaction.readVersionThenCheckAndUpdateInTransactionIfItMutatesZkState(paths.availableWorkPoolVersion())

    fun pruneOutDatedJobs(transaction: ZkTransaction, actualJobs: Set<String>) {
        for (jobIdFromZk in currentJobsFromZk()) {
            if (!actualJobs.contains(jobIdFromZk)) {
                logger.debug { "cleanWorkPool removing $jobIdFromZk" }
                transaction.deletePathWithChildrenIfNeeded(paths.availableWorkPool(jobIdFromZk))
            }
        }
    }

    private fun currentJobsFromZk(): MutableList<String> = curatorFramework.children.forPath(paths.availableWorkPool())

    fun updateAllJobs(transaction: ZkTransaction, newWorkPools: ConcurrentMap<JobDescriptor, WorkPool>) {
        for (job in newWorkPools.keys) {
            val workPoolsPath: String = paths.availableWorkPool(job.jobId.id)
            if (curatorFramework.checkExists().forPath(workPoolsPath) == null) {
                transaction.createPath(workPoolsPath)
            }
            val newWorkPool = newWorkPools[job]!!.items
            updateJob(transaction, job.jobId.id, newWorkPool)
        }
    }

    fun updateJob(transaction: ZkTransaction, jobId: String, newWorkPool: Set<String>) {
        val workPoolFromZk: Set<String> = currentJobWorkPoolFromZk(jobId)
        if (workPoolFromZk != newWorkPool) {
            createItemsContainedInFirstSetButNotInSecond(newWorkPool, workPoolFromZk, transaction, jobId)
            removeItemsContainedInFirstSetButNotInSecond(workPoolFromZk, newWorkPool, transaction, jobId)
        }
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
        newWorkPool: Set<String>, currentWorkPool: Set<String>, transaction: ZkTransaction, jobId: String
    ) {
        val workPoolsToAdd: MutableSet<String> = java.util.HashSet(newWorkPool)
        workPoolsToAdd.removeAll(currentWorkPool)
        for (workItemToAdd in workPoolsToAdd) {
            transaction.createPath(paths.availableWorkItem(jobId, workItemToAdd))
        }
    }

    private fun removeItemsContainedInFirstSetButNotInSecond(
        currentWorkPool: Set<String>, newWorkPool: Set<String>, transaction: ZkTransaction, jobId: String
    ) {
        val workPoolsToDelete: MutableSet<String> = java.util.HashSet(currentWorkPool)
        workPoolsToDelete.removeAll(newWorkPool)
        for (workItemToDelete in workPoolsToDelete) {
            transaction.deletePath(paths.availableWorkItem(jobId, workItemToDelete))
        }
    }

}