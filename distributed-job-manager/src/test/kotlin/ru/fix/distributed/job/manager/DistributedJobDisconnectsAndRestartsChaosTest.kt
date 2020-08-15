package ru.fix.distributed.job.manager

import org.apache.logging.log4j.kotlin.Logging
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule
import java.lang.Thread.sleep
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.ArrayDeque

@ExperimentalStdlibApi
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@Execution(ExecutionMode.CONCURRENT)
class DistributedJobDisconnectsAndRestartsChaosTest : DjmTestSuite() {
    companion object : Logging

    class WorkItemInvocations {
        var previousAccessTime = AtomicReference<Instant>(Instant.now())
        var numberOfInvocations = AtomicInteger(0)
    }

    val workLoad = mapOf(
            "job1" to (1..1).map { "$it" }.toSet(),
            "job3" to (1..3).map { "$it" }.toSet(),
            "job6" to (1..6).map { "$it" }.toSet(),
            "job16" to (1..16).map { "$it" }.toSet())

    val processings = workLoad.map { it.key to it.value.map { workItem -> workItem to WorkItemInvocations() }.toMap() }.toMap()
    val jobs = workLoad.map { ChaosJob(it.key, it.value) }
    val maxDjmsCount = 6

    fun createDjm() = createDJM(jobs)
    fun createAllDjms() = repeat(maxDjmsCount) { createDjm() }


    inner class ChaosJob(val id: String, val workPool: Set<String>) : DistributedJob {
        override val jobId = JobId(id)
        override fun getSchedule() = DynamicProperty.of(Schedule.withRate(50))
        override fun run(context: DistributedJobContext) {
            for (workItem in context.workShare) {
                processings[id]!![workItem]!!.apply {
                    previousAccessTime.set(Instant.now())
                    numberOfInvocations.incrementAndGet()
                }
            }
        }

        override fun getWorkPool() = WorkPool.of(workPool)
        override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
        override fun getWorkPoolCheckPeriod() = 0L
    }

    fun awaitAllWorkItemsAreAccessedDuringOneSecond() =
            await().pollInterval(100, TimeUnit.MILLISECONDS)
                    .atMost(15, TimeUnit.SECONDS)
                    .until {
                        processings.values.flatMap { it.values }.all {
                            Duration.between(it.previousAccessTime.get(), Instant.now()).toSeconds() <= 1
                        }
                    }

    enum class ChaosAction {
        Disconnect, Connect, Close, Start;

        companion object {
            fun rand() = values()[ThreadLocalRandom.current().nextInt(0, values().size)]
        }
    }

    fun printDjmsState() {
        logger.info(djms.map { if (isConnectedDjm(it)) "[v]" else "[x]" }.joinToString())
    }

    fun performRandomAction() {
        for (attempt in 1..10) {
            when (val action = ChaosAction.rand()) {
                ChaosAction.Disconnect -> {
                    val connectedDjms = djms.filter { isConnectedDjm(it) }
                    if (connectedDjms.size >= 2) {
                        disconnectDjm(connectedDjms.random())
                        return
                    }

                }
                ChaosAction.Connect -> {
                    val disconnectedDjms = djms.filter { !isConnectedDjm(it) }
                    if (disconnectedDjms.isNotEmpty()) {
                        connectDjm(disconnectedDjms.random())
                        return
                    }
                }
                ChaosAction.Start -> {
                    if (djms.size < maxDjmsCount) {
                        createDJM(jobs)
                        return
                    }
                }
                ChaosAction.Close -> {
                    val connectedDjms = djms.filter { isConnectedDjm(it) }
                    if (connectedDjms.size >= 2) {
                        closeDjm(connectedDjms.random())
                    }
                }
            }
        }


    }

    @Test
    fun `series of random DJMs disconnects, shutdowns and launches does not affect correct WorkItem launching and schedulling`() {
        createAllDjms()

        val startTime = Instant.now()
        while (Duration.between(startTime, Instant.now()).toSeconds() <= 60) {
            repeat((1..3).random()) {
                performRandomAction()
            }
            printDjmsState()
            sleep(1000)
            awaitAllWorkItemsAreAccessedDuringOneSecond()
        }
    }


    @Test
    fun `series of DJMs restarts one by one does not affect correct WorkItem launching and schedulling`() {
        repeat(3) { createDjm() }
        sleep(1000)
        awaitAllWorkItemsAreAccessedDuringOneSecond()

        val djmsForClosing = ArrayDeque(djms)
        closeDjm(djmsForClosing.removeFirst())
        createDjm()
        sleep(1000)
        awaitAllWorkItemsAreAccessedDuringOneSecond()

        closeDjm(djmsForClosing.removeFirst())
        sleep(1000)
        awaitAllWorkItemsAreAccessedDuringOneSecond()

        createDjm()
        closeDjm(djmsForClosing.removeFirst())
        createDjm()
        sleep(1000)
        awaitAllWorkItemsAreAccessedDuringOneSecond()
    }

    @Test
    fun `series of DJMs restarts two by two does not affect correct WorkItem launching and schedulling`() {
        repeat(6) { createDjm() }
        sleep(1000)
        awaitAllWorkItemsAreAccessedDuringOneSecond()

        val djmsForClosing = ArrayDeque(djms)
        repeat(2) { closeDjm(djmsForClosing.removeFirst()) }
        repeat(2) { createDjm() }
        sleep(1000)
        awaitAllWorkItemsAreAccessedDuringOneSecond()

        repeat(2) { closeDjm(djmsForClosing.removeFirst()) }
        sleep(1000)
        awaitAllWorkItemsAreAccessedDuringOneSecond()

        repeat(2) { createDjm() }
        repeat(2) { closeDjm(djmsForClosing.removeFirst()) }
        repeat(2) { createDjm() }
        sleep(1000)
        awaitAllWorkItemsAreAccessedDuringOneSecond()
    }
}

