package ru.fix.distributed.job.manager

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.PathChildrenCache
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

internal class Cleaner(
        profiler: Profiler,
        private val paths: ZkPathsManager,
        private val curatorFramework: CuratorFramework,
        private val leaderLatchExecutor: LeaderLatchExecutor
) : AutoCloseable {
    private val scheduler = NamedExecutors.newSingleThreadScheduler(
            "work-pool cleaning task",
            profiler
    )
    private val workPoolSubTree = AvailableWorkPoolSubTree(curatorFramework, paths)
    private val zkPrinter = ZkTreePrinter(curatorFramework)

    fun startWorkPoolCleaningTask(
            initializedAliveWorkersCache: PathChildrenCache,
            workPoolCleanPeriod: DynamicProperty<Long>
    ): ScheduledFuture<*>? {
        return scheduler.schedule(Schedule.withDelay(workPoolCleanPeriod), workPoolCleanPeriod.get()) {
            try {
                if (leaderLatchExecutor.hasLeadershipAndNotShutdown()) {
                    ZkTransaction.tryCommit(
                            curatorFramework,
                            CLEAN_WORK_POOL_RETRIES_COUNT
                    ) { transaction ->
                        cleanWorkPool(transaction, initializedAliveWorkersCache)
                    }
                }
            } catch (e: Exception) {
                logger.debug("Failed to clean work-pool", e)
            }
        }
    }

    private fun cleanWorkPool(transaction: ZkTransaction, initializedAliveWorkersCache: PathChildrenCache) {
        workPoolSubTree.checkAndUpdateVersion(transaction)
        logger.trace { "cleanWorkPool zk tree before cleaning: ${zkPrinter.print(paths.rootPath)}" }
        val actualJobs: MutableSet<String> = HashSet()
        val aliveWorkersPath: String = paths.aliveWorkers()
        for (aliveWorkerNodeData in initializedAliveWorkersCache.currentData) {
            val aliveWorkerPath = aliveWorkerNodeData.path
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