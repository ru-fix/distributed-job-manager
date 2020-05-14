package ru.fix.distributed.job.manager

import mu.KotlinLogging
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

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
            log.error("waiting event was interrupted", e)
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
            log.warn("Failed to wait eventReceivingExecutor termination")
            eventReceivingExecutor.shutdownNow()
        }
    }

    companion object {
        private val log = KotlinLogging.logger {}
    }
}