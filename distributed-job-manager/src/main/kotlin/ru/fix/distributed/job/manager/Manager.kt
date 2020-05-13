package ru.fix.distributed.job.manager

import mu.KotlinLogging
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.PathChildrenCache
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener
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

    private val leaderLatchExecutor = LeaderLatchExecutor(
            profiler, paths, curatorFramework
    )
    private val cleaner = Cleaner(
            profiler, paths, curatorFramework, leaderLatchExecutor
    )
    private val rebalancer = Rebalancer(
            paths, curatorFramework, leaderLatchExecutor, settings.assignmentStrategy, settings.nodeId
    )

    private val workersAliveChildrenCache = PathChildrenCache(
            curatorFramework,
            paths.aliveWorkers(),
            false
    )
    private val nodeId = settings.nodeId
    private val workPoolCleanPeriod = settings.workPoolCleanPeriod


    fun start() {
        workersAliveChildrenCache.listenable.addListener(PathChildrenCacheListener { _: CuratorFramework, event: PathChildrenCacheEvent ->
            log.trace("nodeId=$nodeId handleRebalanceEvent event=$event")
            when (event.type) {
                PathChildrenCacheEvent.Type.CONNECTION_RECONNECTED,
                PathChildrenCacheEvent.Type.CHILD_UPDATED,
                PathChildrenCacheEvent.Type.CHILD_ADDED,
                PathChildrenCacheEvent.Type.CHILD_REMOVED ->
                    rebalancer.enqueueRebalance()
                PathChildrenCacheEvent.Type.CONNECTION_SUSPENDED -> {
                }
                PathChildrenCacheEvent.Type.INITIALIZED -> cleaner.startWorkPoolCleaningTask(
                        workersAliveChildrenCache,
                        workPoolCleanPeriod
                )
                else -> log.warn("nodeId=$nodeId Invalid event type ${event.type}")
            }
        })
        workersAliveChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT)

        leaderLatchExecutor.addLeadershipListener {
            log.info("nodeId=$nodeId became a leader")
            rebalancer.enqueueRebalance()
        }
        leaderLatchExecutor.start()

        rebalancer.start()
    }

    override fun close() {
        val managerStopTime = System.currentTimeMillis()
        log.info("Closing DJM manager entity...")

        workersAliveChildrenCache.close()
        leaderLatchExecutor.close()
        rebalancer.close()
        cleaner.close()

        log.info("DJM manager was closed. Took ${System.currentTimeMillis() - managerStopTime} ms")
    }

    companion object {
        private val log = KotlinLogging.logger {}
    }
}