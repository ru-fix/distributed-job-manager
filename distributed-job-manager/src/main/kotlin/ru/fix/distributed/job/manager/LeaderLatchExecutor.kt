package ru.fix.distributed.job.manager

import org.apache.curator.framework.recipes.leader.LeaderLatch
import org.apache.curator.framework.recipes.leader.LeaderLatchListener
import org.apache.logging.log4j.kotlin.Logging
import ru.fix.aggregating.profiler.Profiler
import ru.fix.stdlib.concurrency.threads.NamedExecutors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit

internal class LeaderLatchExecutor(
        profiler: Profiler,
        private val leaderLatch: LeaderLatch
) : AutoCloseable {
    private val executor = NamedExecutors.newSingleThreadPool(
            "distributed-manager-thread",
            profiler
    )

    fun start() = leaderLatch.start()

    fun submitIfNeeded(task: Runnable): Future<*>? = synchronized(executor) {
        if (!executor.isShutdown && leaderLatch.hasLeadership()) {
            return executor.submit(task)
        }
        return null
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
            logger.error("Failed to wait LeaderLatchExecutor executor termination")
            executor.shutdownNow()
        }
    }

    companion object : Logging

}