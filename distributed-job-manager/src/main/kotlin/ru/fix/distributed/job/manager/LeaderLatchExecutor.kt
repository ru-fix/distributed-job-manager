package ru.fix.distributed.job.manager

import mu.KotlinLogging
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.LeaderLatch
import org.apache.curator.framework.recipes.leader.LeaderLatchListener
import ru.fix.aggregating.profiler.Profiler
import ru.fix.stdlib.concurrency.threads.NamedExecutors
import java.util.concurrent.TimeUnit

internal class LeaderLatchExecutor(
        profiler: Profiler,
        paths: ZkPathsManager,
        curatorFramework: CuratorFramework
) : AutoCloseable {
    private val executor = NamedExecutors.newSingleThreadPool("distributed-manager-thread", profiler)
    private val leaderLatch = LeaderLatch(curatorFramework, paths.leaderLatch())

    fun start() = leaderLatch.start()

    fun tryExecute(task: Runnable) = synchronized(executor) {
        if (!executor.isShutdown && leaderLatch.hasLeadership()) {
            executor.execute(task)
        }
    }

    fun hasLeadershipAndNotShutdown(): Boolean = synchronized(executor) {
        return leaderLatch.hasLeadership() && !executor.isShutdown
    }

    fun isShutdown(): Boolean = synchronized(executor) {
        return executor.isShutdown
    }

    fun addLeadershipListener(isLeader: () -> Unit) {
        leaderLatch.addListener(object : LeaderLatchListener {
            override fun isLeader() = isLeader.invoke()

            override fun notLeader() {
                // Do nothing when leadership is lost
            }
        })
    }

    override fun close() {
        if (leaderLatch.state == LeaderLatch.State.STARTED) {
            leaderLatch.close()
        }
        synchronized(executor) {
            executor.shutdown()
        }
        if (!executor.awaitTermination(3, TimeUnit.MINUTES)) {
            log.error("Failed to wait LeaderLatchExecutor thread pool termination")
            executor.shutdownNow()
        }
    }

    companion object {
        private val log = KotlinLogging.logger {}
    }

}