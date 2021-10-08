package ru.fix.distributed.job.manager

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.CuratorCache
import org.apache.logging.log4j.kotlin.Logging
import ru.fix.aggregating.profiler.Profiler
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.NamedExecutors
import ru.fix.stdlib.concurrency.threads.Schedule
import ru.fix.zookeeper.transactional.ZkTransaction
import ru.fix.zookeeper.utils.ZkTreePrinter
import java.util.*
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

private const val CLEAN_WORK_POOL_RETRIES_COUNT = 1

/**
 * Removes in background irrelevant jobs from /work-pool/ subtree, if [managerState] allows.
 * Job is irrelevant if there are no worker that could run this job.
 * */
internal class Cleaner(
    profiler: Profiler,
    private val paths: ZkPathsManager,
    private val curatorFramework: CuratorFramework,
    private val managerState: DynamicProperty<Manager.State>,
    private val workPoolCleanPeriod: DynamicProperty<Long>,
    private val aliveWorkersCache: CuratorCache
) : AutoCloseable {
    private val scheduler = NamedExecutors.newSingleThreadScheduler("cleaning-task", profiler)
    private val workPoolSubTree = AvailableWorkPoolSubTree(curatorFramework, paths)
    private val zkPrinter = ZkTreePrinter(curatorFramework)

    fun start(): ScheduledFuture<*>? {
        return scheduler.schedule(Schedule.withDelay(workPoolCleanPeriod), workPoolCleanPeriod) {
            try {
                if (managerState.get() == Manager.State.IS_LEADER) {
                    ZkTransaction.tryCommit(
                        curatorFramework,
                        CLEAN_WORK_POOL_RETRIES_COUNT
                    ) { transaction ->
                        cleanWorkPool(transaction)
                    }
                }
            } catch (e: Exception) {
                logger.debug("Failed to clean work-pool", e)
            }
        }
    }

    private fun cleanWorkPool(transaction: ZkTransaction) {
        workPoolSubTree.readVersionThenCheckAndUpdateIfTxMutatesState(transaction)
        logger.trace { "cleanWorkPool zk tree before cleaning: ${zkPrinter.print(paths.rootPath)}" }
        val actualJobs: MutableSet<String> = HashSet()
        val aliveWorkersPath: String = paths.aliveWorkers()
        for (aliveWorkerNodeData in aliveWorkersCache.stream()) {
            val aliveWorkerPath = aliveWorkerNodeData.path
            if (aliveWorkerPath == aliveWorkersPath) {
                continue // skip parent node "workers"
            }
            // getting "worker-id" from "workers/worker-id"
            val workerId = aliveWorkerPath.substring(aliveWorkersPath.length + 1)
            actualJobs.addAll(curatorFramework.children.forPath(paths.availableJobs(workerId)))
        }
        workPoolSubTree.pruneOutDatedJobs(transaction, actualJobs)
    }

    override fun close() {
        scheduler.shutdown()
        if (!scheduler.awaitTermination(1, TimeUnit.MINUTES)) {
            logger.warn("Failed to wait cleaner's scheduler termination")
            scheduler.shutdownNow()
        }
    }

    companion object : Logging
}