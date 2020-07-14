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
import ru.fix.dynamic.property.api.AtomicProperty
import ru.fix.stdlib.concurrency.events.ReducingEventAccumulator
import ru.fix.stdlib.concurrency.threads.NamedExecutors
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

    private val currentState = AtomicProperty(State.IS_NOT_LEADER)

    private val cleaner = Cleaner(
            profiler, paths, curatorFramework, currentState, settings.workPoolCleanPeriod, aliveWorkersCache
    )
    private val rebalancer = Rebalancer(
            paths, curatorFramework, currentState, settings.assignmentStrategy, nodeId
    )

    private val rebalanceExecutor = NamedExecutors.newSingleThreadPool("rebalance_thread", profiler)
    private val rebalanceAccumulator =
            ReducingEventAccumulator.lastEventWinAccumulator<RebalanceTrigger>()

    fun start() {
        initCuratorCacheForManagerEvents(aliveWorkersCache, paths.aliveWorkers())
        initLeaderLatchForManagerEvents()

        cleaner.start()
        startRebalancingTask()
    }

    private fun startRebalancingTask() {
        rebalanceExecutor.execute {
            while (currentState.get() != State.SHUTDOWN) {
                if (rebalanceAccumulator.extractAccumulatedValueOrNull() != null) {
                    rebalancer.reassignAndBalanceTasks()
                }
            }
        }
    }

    private fun initLeaderLatchForManagerEvents() {
        leaderLatch.addListener(object : LeaderLatchListener {
            override fun notLeader() {
                logger.info { "nodeId=$nodeId lost a leadership" }
                handleManagerEvent(ManagerEvent.LEADERSHIP_LOST)
            }

            override fun isLeader() {
                logger.info { "nodeId=$nodeId became a leader" }
                handleManagerEvent(ManagerEvent.LEADERSHIP_ACQUIRED)
            }
        })
        leaderLatch.start()
    }

    private fun initCuratorCacheForManagerEvents(cache: CuratorCache, treeName: String) {
        val cacheInitLocker = Semaphore(0)
        cache.listenable().addListener(object : CuratorCacheListener {
            override fun event(type: CuratorCacheListener.Type?, oldData: ChildData?, data: ChildData?) {
                logger.trace { "nodeId=$nodeId $treeName event: type=$type, oldData=$oldData, data=$data" }
                handleManagerEvent(ManagerEvent.ZK_WORKERS_CONFIG_CHANGED)
            }

            override fun initialized() {
                cacheInitLocker.release()
            }
        })
        cache.start()
        cacheInitLocker.acquire()
    }

    private fun handleManagerEvent(newEvent: ManagerEvent) {
        when (currentState.get()!!) {
            State.IS_NOT_LEADER -> handleManagerEventAsNonLeader(newEvent)
            State.IS_LEADER -> handleManagerEventAsLeader(newEvent)
            State.SHUTDOWN -> {
            }
        }
    }

    private fun handleManagerEventAsLeader(event: ManagerEvent) {
        when (event) {
            ManagerEvent.ZK_WORKERS_CONFIG_CHANGED -> {
                rebalanceAccumulator.publishEvent(RebalanceTrigger.DO_REBALANCE)
            }
            ManagerEvent.LEADERSHIP_LOST -> {
                currentState.set(State.IS_NOT_LEADER)
            }
            ManagerEvent.SHUTDOWN -> {
                currentState.set(State.SHUTDOWN)
            }
            ManagerEvent.LEADERSHIP_ACQUIRED -> {
                logger.warn { "received ${ManagerEvent.LEADERSHIP_ACQUIRED} event, but manager is already the leader" }
            }
        }
    }

    private fun handleManagerEventAsNonLeader(event: ManagerEvent) {
        when (event) {
            ManagerEvent.ZK_WORKERS_CONFIG_CHANGED -> {
            }
            ManagerEvent.LEADERSHIP_ACQUIRED -> {
                currentState.set(State.IS_LEADER)
                rebalanceAccumulator.publishEvent(RebalanceTrigger.DO_REBALANCE)
            }
            ManagerEvent.SHUTDOWN -> {
                currentState.set(State.SHUTDOWN)
            }
            ManagerEvent.LEADERSHIP_LOST -> {
                logger.warn { "received ${ManagerEvent.LEADERSHIP_LOST} event, but manager has already lost leadership" }
            }
        }
    }

    override fun close() {
        val managerStopTime = System.currentTimeMillis()
        logger.info("Closing DJM manager entity...")

        handleManagerEvent(ManagerEvent.SHUTDOWN)

        aliveWorkersCache.close()
        leaderLatch.close()
        cleaner.close()

        logger.info { "DJM manager was closed. Took ${System.currentTimeMillis() - managerStopTime} ms" }
    }

    private enum class ManagerEvent {
        ZK_WORKERS_CONFIG_CHANGED, LEADERSHIP_LOST, LEADERSHIP_ACQUIRED, SHUTDOWN
    }

    private enum class RebalanceTrigger {
        DO_REBALANCE
    }

    enum class State {
        IS_LEADER, IS_NOT_LEADER, SHUTDOWN
    }

    companion object : Logging
}
