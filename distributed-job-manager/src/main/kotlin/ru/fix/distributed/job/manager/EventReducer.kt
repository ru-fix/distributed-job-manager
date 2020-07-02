package ru.fix.distributed.job.manager

import org.apache.logging.log4j.kotlin.Logging
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * EventReducer invokes given [handler] when [handle] function was invoked.
 * If [handle] function was invoked (one or many times) during [handler] working,
 * [handler] will be invoked after completing and so on.
 *
 * Therefore if [handle] function was invoked 100 times with breaks in 1 millisecond,
 * and [handler]'s work takes 70 milliseconds,
 * then [handler] wasn't invoked 100 times, but 3 times, consistently.
 * */
class EventReducer(
        private val handler: () -> Unit,
        private val shutdownCheckPeriodMs: Long = 1_000,
        private val awaitTerminationPeriodMs: Long = 60_000
) : AutoCloseable {
    private val eventReceivingExecutor = Executors.newSingleThreadExecutor()

    private val awaitingEventQueue = ArrayBlockingQueue<Any>(1)

    fun handle() = synchronized(awaitingEventQueue) {
        if (awaitingEventQueue.isEmpty()) {
            awaitingEventQueue.put(Any())
        }
    }

    fun start() {
        CompletableFuture.runAsync(Runnable {
            while (true) {
                when (awaitEventOrShutdown()) {
                    AwaitingResult.EVENT -> handler.invoke()
                    AwaitingResult.SHUTDOWN -> return@Runnable
                    AwaitingResult.ERROR -> {
                    }
                }
            }
        }, eventReceivingExecutor)
    }

    enum class AwaitingResult {
        EVENT, SHUTDOWN, ERROR
    }

    private fun awaitEventOrShutdown(): AwaitingResult {
        try {
            while (awaitingEventQueue.poll(shutdownCheckPeriodMs, TimeUnit.MILLISECONDS) == null) {
                if (eventReceivingExecutor.isShutdown) {
                    return AwaitingResult.SHUTDOWN
                }
            }
        } catch (e: Exception) {
            logger.error("waiting event was interrupted", e)
            return AwaitingResult.ERROR
        } finally {
            synchronized(awaitingEventQueue) {
                awaitingEventQueue.clear()
            }
        }
        return AwaitingResult.EVENT
    }

    override fun close() {
        eventReceivingExecutor.shutdown()
        if (!eventReceivingExecutor.awaitTermination(awaitTerminationPeriodMs, TimeUnit.MILLISECONDS)) {
            logger.warn("Failed to wait eventReceivingExecutor termination")
            eventReceivingExecutor.shutdownNow()
        }
    }

    companion object : Logging
}