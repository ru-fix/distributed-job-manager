package ru.fix.distributed.job.manager

import io.kotest.matchers.booleans.shouldBeFalse
import org.apache.logging.log4j.kotlin.Logging
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.*
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule
import java.lang.Thread.sleep
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference


@ExperimentalStdlibApi
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
//Prevent log messages from different tests to mix
@Execution(ExecutionMode.SAME_THREAD)
class DistributedJobDisconnectsAndRestartsTest : DjmTestSuite() {
    companion object : Logging

    @Test
    fun `during disconnect of DJM1, DJM2 does not steal WorkItem that currently under work by DJM1`() {
        val workItems = (1..10).map { it.toString() }.toSet()

        val job = object : DistributedJob {
            val isWorkItemConflictDetected = AtomicBoolean(false)
            val workShareInUse = ConcurrentHashMap.newKeySet<String>()
            override val jobId = JobId("conflicting-job")
            override fun getSchedule() = DynamicProperty.of(Schedule.withRate(50))
            override fun run(context: DistributedJobContext) {
                for (workItem in context.workShare) {
                    if (!workShareInUse.add(workItem)) {
                        isWorkItemConflictDetected.set(true)
                    }
                }
                sleep(500)
                workShareInUse.removeAll(context.workShare)
            }

            override fun getWorkPool() = WorkPool.of(workItems)
            override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
            override fun getWorkPoolCheckPeriod() = 0L
        }

        val rootPath = generateDjmRootPath()
        val djm1 = createDJM(job, rootPath = rootPath)
        val djm2 = createDJM(job, rootPath = rootPath)
        sleep(1000)
        job.isWorkItemConflictDetected.get().shouldBeFalse()

        disconnectDjm(djm1)
        sleep(1000)
        job.isWorkItemConflictDetected.get().shouldBeFalse()

        connectDjm(djm1)
        sleep(1000)
        job.isWorkItemConflictDetected.get().shouldBeFalse()

        closeDjm(djm1)
        sleep(1000)
        job.isWorkItemConflictDetected.get().shouldBeFalse()

        closeDjm(djm2)
        sleep(500)
        job.isWorkItemConflictDetected.get().shouldBeFalse()
    }

    @Test
    fun `when DJM3 disconnects, WorkItems rebalanced between DJM1 and DJM2`() {
        val workItems = (1..10).map { it.toString() }.toSet()

        class LastUsedWorkShareJob : DistributedJob {
            val lastUsedWorkShare = AtomicReference<Set<String>>(emptySet())
            override val jobId = JobId("rebalance-on-disconnect-job")
            override fun getSchedule() = DynamicProperty.of(Schedule.withRate(100))
            override fun run(context: DistributedJobContext) {
                lastUsedWorkShare.set(context.workShare)
            }

            override fun getWorkPool() = WorkPool.of(workItems)
            override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
            override fun getWorkPoolCheckPeriod() = 0L
        }

        val job1 = LastUsedWorkShareJob()
        val job2 = LastUsedWorkShareJob()
        val job3 = LastUsedWorkShareJob()

        val rootPath = generateDjmRootPath()
        val djm1 = createDJM(job1, rootPath = rootPath)
        val djm2 = createDJM(job2, rootPath = rootPath)
        val djm3 = createDJM(job3, rootPath = rootPath)

        await().pollDelay(100, TimeUnit.MILLISECONDS)
                .atMost(15, TimeUnit.SECONDS)
                .until {
                    job1.lastUsedWorkShare.get().isNotEmpty() &&
                            job2.lastUsedWorkShare.get().isNotEmpty() &&
                            job3.lastUsedWorkShare.get().isNotEmpty() &&

                            (job1.lastUsedWorkShare.get() +
                                    job2.lastUsedWorkShare.get() +
                                    job3.lastUsedWorkShare.get()).toSet().size == 10
                }

        disconnectDjm(djm3)

        await().pollDelay(100, TimeUnit.MILLISECONDS)
                .atMost(15, TimeUnit.SECONDS)
                .until {
                    job1.lastUsedWorkShare.get().isNotEmpty() &&
                            job2.lastUsedWorkShare.get().isNotEmpty() &&
                            (job1.lastUsedWorkShare.get() + job2.lastUsedWorkShare.get()).toSet().size == 10
                }

        connectDjm(djm3)

        closeDjm(djm1)
        closeDjm(djm2)
        closeDjm(djm3)
    }

    @Test
    fun `when DJM3 shutdowns, WorkItems rebalanced between DJM1 and DJM2`() {
        val workItems = (1..10).map { it.toString() }.toSet()

        class LastUsedWorkShareJob : DistributedJob {
            val lastUsedWorkShare = AtomicReference<Set<String>>(emptySet())
            override val jobId = JobId("rebalance-on-disconnect-job")
            override fun getSchedule() = DynamicProperty.of(Schedule.withRate(100))
            override fun run(context: DistributedJobContext) {
                lastUsedWorkShare.set(context.workShare)
            }

            override fun getWorkPool() = WorkPool.of(workItems)
            override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
            override fun getWorkPoolCheckPeriod() = 0L
        }

        val job1 = LastUsedWorkShareJob()
        val job2 = LastUsedWorkShareJob()
        val job3 = LastUsedWorkShareJob()

        val rootPath = generateDjmRootPath()
        val djm1 = createDJM(job1, rootPath = rootPath)
        val djm2 = createDJM(job2, rootPath = rootPath)
        val djm3 = createDJM(job3, rootPath = rootPath)

        await().pollDelay(100, TimeUnit.MILLISECONDS)
                .atMost(15, TimeUnit.SECONDS)
                .until {
                    job1.lastUsedWorkShare.get().isNotEmpty() &&
                            job2.lastUsedWorkShare.get().isNotEmpty() &&
                            job3.lastUsedWorkShare.get().isNotEmpty() &&

                            (job1.lastUsedWorkShare.get() +
                                    job2.lastUsedWorkShare.get() +
                                    job3.lastUsedWorkShare.get()).toSet().size == 10
                }

        closeDjm(djm3)

        await().pollDelay(100, TimeUnit.MILLISECONDS)
                .atMost(15, TimeUnit.SECONDS)
                .until {
                    job1.lastUsedWorkShare.get().isNotEmpty() &&
                            job2.lastUsedWorkShare.get().isNotEmpty() &&
                            (job1.lastUsedWorkShare.get() + job2.lastUsedWorkShare.get()).toSet().size == 10
                }

        closeDjm(djm1)
        closeDjm(djm2)
    }


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
    val rootPath = generateDjmRootPath()
    val maxDjmsCount = 6
    fun createDjm() = createDJM(jobs, rootPath = rootPath)


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
                        createDJM(jobs, rootPath = rootPath)
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
        repeat(6) { createDjm() }

        val startTime = Instant.now()
        while (Duration.between(startTime, Instant.now()).toSeconds() <= 60) {
            repeat((1..3).random()) {
                performRandomAction()
            }
            printDjmsState()
            sleep(1100)
            awaitAllWorkItemsAreAccessedDuringOneSecond()
        }
    }


    @Disabled("TODO")
    @Test
    fun `series of DJMs restarts one by one does not affect correct WorkItem launching and schedulling`() {
        sleep(1000)
        TODO()
    }

    @Disabled("TODO")
    @Test
    fun `series of DJMs restarts two by two does not affect correct WorkItem launching and schedulling`() {
        sleep(1000)
        TODO()
    }

    @Disabled("TODO")
    @Test
    fun `series of DJMs restarts three by three does not affect correct WorkItem launching and schedulling`() {
        sleep(1000)
        TODO()
    }

    @Disabled("TODO")
    @Test
    fun `Change in Job WorkPool triggers rebalance`() {
        sleep(1000)
        TODO()
    }

    @Disabled("TODO")
    @Test
    fun `Check WorkPool period small, no change in WorkPool then should be no change to zk and no rebalance is triggered`() {
        sleep(1000)
        TODO()
    }

    @Disabled("TODO")
    @Test
    fun `If WorkPool and number of DJMs does not change, no rebalance is triggered `() {
        sleep(1000)
        TODO()
    }

    @Disabled("TODO")
    @Test
    fun `DJM shutdown triggers rebalance in cluster `() {
        sleep(1000)
        TODO()
    }


    @Disabled("TODO")
    @Test
    fun `DJM set of available jobs changes triggers rebalance in cluster `() {
        sleep(1000)
        TODO()

    }

    @Disabled("TODO")
    @Test
    fun `DJM follows assignment strategy`() {
        sleep(1000)
        TODO()
    }

    @Disabled("TODO")
    @Test
    fun `Assignment strategy that assign same workItem to different workers rise an exception`() {
        sleep(1000)
        TODO()
    }

    @Disabled("TODO")
    @Test
    fun `DJM does not allow two jobs with same ID`() {
        sleep(1000)
        TODO()
    }

    @Disabled("TODO")
    @Test
    fun `DJM does not allow incorrect symbols in WorkPool`() {
        sleep(1000)
        TODO()
    }


}

