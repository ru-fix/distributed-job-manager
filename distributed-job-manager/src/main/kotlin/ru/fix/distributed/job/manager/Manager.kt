package ru.fix.distributed.job.manager

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.ChildData
import org.apache.curator.framework.recipes.cache.CuratorCache
import org.apache.curator.framework.recipes.cache.CuratorCacheListener
import org.apache.curator.framework.recipes.leader.LeaderLatch
import org.apache.logging.log4j.kotlin.Logging
import ru.fix.aggregating.profiler.Profiler
import ru.fix.distributed.job.manager.model.DistributedJobManagerSettings
import java.util.concurrent.Semaphore

/**
 * Only single manager is active on the cluster.
 * Manages job assignments on cluster by modifying assignment section of zookeeper tree.
 *
 * @author Kamil Asfandiyarov
 * @see Worker
 */
class Manager(
        curatorFramework: CuratorFramework,
        profiler: Profiler,
        settings: DistributedJobManagerSettings
) : AutoCloseable {
    private val paths = ZkPathsManager(settings.rootPath)
    private val nodeId = settings.nodeId

    private val aliveWorkersCache = CuratorCache
            .bridgeBuilder(curatorFramework, paths.aliveWorkers())
            .withDataNotCached()
            .build()

    private val leaderLatchExecutor = LeaderLatchExecutor(
            profiler, LeaderLatch(curatorFramework, paths.leaderLatch())
    )
    private val cleaner = Cleaner(
            profiler, paths, curatorFramework, leaderLatchExecutor, settings.workPoolCleanPeriod, aliveWorkersCache
    )
    private val rebalancer = Rebalancer(
            profiler, paths, curatorFramework, leaderLatchExecutor, settings.assignmentStrategy, nodeId
    )

    fun start() {
        val aliveWorkersCacheInitLocker = Semaphore(0)
        leaderLatchExecutor.addCuratorCacheListener(aliveWorkersCache, object : CuratorCacheListener {
            override fun event(type: CuratorCacheListener.Type?, oldData: ChildData?, data: ChildData?) {
                logger.trace { "nodeId=$nodeId aliveWorkersCache rebalance event: type=$type, oldData=$oldData, data=$data" }
                rebalancer.handleRebalanceEvent()
            }

            override fun initialized() {
                aliveWorkersCacheInitLocker.release()
            }
        })
        aliveWorkersCache.start()

        leaderLatchExecutor.addLeadershipListener {
            logger.info { "nodeId=$nodeId became a leader" }
            rebalancer.handleRebalanceEvent()
        }
        leaderLatchExecutor.start()

        aliveWorkersCacheInitLocker.acquire()

        cleaner.start()
        rebalancer.start()
    }

    override fun close() {
        val managerStopTime = System.currentTimeMillis()
        logger.info("Closing DJM manager entity...")

        aliveWorkersCache.close()
        leaderLatchExecutor.close()
        rebalancer.close()
        cleaner.close()

        logger.info { "DJM manager was closed. Took ${System.currentTimeMillis() - managerStopTime} ms" }
    }

    companion object : Logging
}