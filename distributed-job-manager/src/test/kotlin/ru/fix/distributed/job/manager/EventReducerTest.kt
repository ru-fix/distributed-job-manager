package ru.fix.distributed.job.manager

import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

internal class EventReducerTest {

    @Test
    fun `WHEN handle single time THEN handler invoked one time`() {
        val invokes = ArrayBlockingQueue<Any>(1)
        EventReducer(handler = {
            invokes.put(Any())
        }).use {
            it.start()
            it.handle()
            assertNotNull(invokes.poll(1, TimeUnit.SECONDS))
            assertNull(invokes.poll(1, TimeUnit.SECONDS))
        }
    }

    @Test
    fun `WHEN reducer triggered several times at once AND handler is slow THEN handler invoked one or two times`() {
        val eventsQuantity = 10
        val invokes = ArrayBlockingQueue<Any>(2)
        EventReducer(handler = {
            invokes.put(Any())
            Thread.sleep(1_000)
        }).use { reducer ->
            reducer.start()

            val executor = Executors.newFixedThreadPool(eventsQuantity)
            val countDownLatch = CountDownLatch(eventsQuantity)
            repeat(eventsQuantity) {
                executor.execute {
                    countDownLatch.countDown()
                    countDownLatch.await()
                    reducer.handle()
                }
            }

            assertNotNull(invokes.poll(1, TimeUnit.SECONDS))
            invokes.poll(1, TimeUnit.SECONDS)
            assertNull(invokes.poll(1, TimeUnit.SECONDS))

            executor.shutdown()
        }
    }

    @Test
    fun `WHEN handle event after invoking handler THEN handler will be invoked after completing`() {
        val invokes = ArrayBlockingQueue<Any>(1)
        val holdHandlerInvocationLock = ReentrantLock().apply { lock() }
        EventReducer(handler = {
            invokes.put(Any())
            holdHandlerInvocationLock.lock()
        }).use { reducer ->
            reducer.start()
            reducer.handle()
            assertNotNull(invokes.poll(1, TimeUnit.SECONDS))
            reducer.handle()
            holdHandlerInvocationLock.unlock()
            assertNotNull(invokes.poll(1, TimeUnit.SECONDS))
        }
    }
}