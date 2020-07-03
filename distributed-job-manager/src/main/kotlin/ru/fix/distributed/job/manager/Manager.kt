package ru.fix.distributed.job.manager

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.PathChildrenCache
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener
import org.apache.curator.framework.recipes.leader.LeaderLatch
import org.apache.logging.log4j.kotlin.Logging
import ru.fix.aggregating.profiler.Profiler
import ru.fix.distributed.job.manager.model.DistributedJobManagerSettings

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
    private val workPoolCleanPeriod = settings.workPoolCleanPeriod

    private val leaderLatchExecutor = LeaderLatchExecutor(
            profiler, LeaderLatch(curatorFramework, paths.leaderLatch())
    )
    private val cleaner = Cleaner(
            profiler, paths, curatorFramework, leaderLatchExecutor
    )
    private val rebalancer = Rebalancer(
            profiler, paths, curatorFramework, leaderLatchExecutor, settings.assignmentStrategy, nodeId
    )

    private val workersAliveChildrenCache = PathChildrenCache(
            curatorFramework,
            paths.aliveWorkers(),
            false
    )


    fun start() {
        workersAliveChildrenCache.listenable.addListener(PathChildrenCacheListener { _: CuratorFramework, event: PathChildrenCacheEvent ->
            logger.trace { "nodeId=$nodeId handleRebalanceEvent event=$event" }
            when (event.type) {
                PathChildrenCacheEvent.Type.CONNECTION_RECONNECTED,
                PathChildrenCacheEvent.Type.CHILD_UPDATED,
                PathChildrenCacheEvent.Type.CHILD_ADDED,
                PathChildrenCacheEvent.Type.CHILD_REMOVED ->
                    rebalancer.handleRebalanceEvent()
                PathChildrenCacheEvent.Type.CONNECTION_SUSPENDED -> {
                }
                PathChildrenCacheEvent.Type.INITIALIZED -> cleaner.startWorkPoolCleaningTask(
                        workersAliveChildrenCache,
                        workPoolCleanPeriod
                )
                else -> logger.warn { "nodeId=$nodeId Invalid event type ${event.type}" }
            }
        })
        workersAliveChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT)

        leaderLatchExecutor.addLeadershipListener {
            logger.info { "nodeId=$nodeId became a leader" }
            rebalancer.handleRebalanceEvent()
        }
        leaderLatchExecutor.start()

        rebalancer.start()
    }

    override fun close() {
        val managerStopTime = System.currentTimeMillis()
        logger.info("Closing DJM manager entity...")

        workersAliveChildrenCache.close()
        leaderLatchExecutor.close()
        rebalancer.close()
        cleaner.close()

        logger.info { "DJM manager was closed. Took ${System.currentTimeMillis() - managerStopTime} ms" }
    }

    companion object : Logging
}