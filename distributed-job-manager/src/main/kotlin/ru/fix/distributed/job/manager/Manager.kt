package ru.fix.distributed.job.manager

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.ChildData
import org.apache.curator.framework.recipes.cache.CuratorCache
import org.apache.curator.framework.recipes.cache.CuratorCacheListener
import org.apache.curator.framework.recipes.leader.LeaderLatch
import org.apache.curator.framework.recipes.leader.LeaderLatchListener
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

    private val leaderLatch = LeaderLatch(curatorFramework, paths.leaderLatch())

    private val managerState = ManagerState()

    private val cleaner = Cleaner(
            profiler, paths, curatorFramework, managerState, settings.workPoolCleanPeriod, aliveWorkersCache
    )
    private val rebalancer = Rebalancer(
            profiler, paths, curatorFramework, managerState, settings.assignmentStrategy, nodeId
    )

    fun start() {
        initCuratorCacheForManagerEvents(aliveWorkersCache, paths.aliveWorkers())
        initLeaderLatchForManagerEvents()

        cleaner.start()
        rebalancer.start()
    }

    private fun initLeaderLatchForManagerEvents() {
        leaderLatch.addListener(object : LeaderLatchListener {
            override fun notLeader() {
                logger.info { "nodeId=$nodeId lost a leadership" }
                managerState.publishEvent(ManagerEvent.LEADERSHIP_LOST)
            }

            override fun isLeader() {
                logger.info { "nodeId=$nodeId became a leader" }
                managerState.publishEvent(ManagerEvent.LEADERSHIP_ACQUIRED)
            }
        })
        leaderLatch.start()
    }

    private fun initCuratorCacheForManagerEvents(cache: CuratorCache, treeName: String) {
        val cacheInitLocker = Semaphore(0)
        cache.listenable().addListener(object : CuratorCacheListener {
            override fun event(type: CuratorCacheListener.Type?, oldData: ChildData?, data: ChildData?) {
                logger.trace { "nodeId=$nodeId $treeName event: type=$type, oldData=$oldData, data=$data" }
                managerState.publishEvent(ManagerEvent.COMMON_REBALANCE_EVENT)
            }

            override fun initialized() {
                cacheInitLocker.release()
            }
        })
        cache.start()
        cacheInitLocker.acquire()
    }

    override fun close() {
        val managerStopTime = System.currentTimeMillis()
        logger.info("Closing DJM manager entity...")

        managerState.publishEvent(ManagerEvent.SHUTDOWN)

        aliveWorkersCache.close()
        leaderLatch.close()
        rebalancer.close()
        cleaner.close()

        logger.info { "DJM manager was closed. Took ${System.currentTimeMillis() - managerStopTime} ms" }
    }

    companion object : Logging
}