package ru.fix.distributed.job.manager.djm

import io.kotest.matchers.booleans.shouldBeFalse
import org.apache.logging.log4j.kotlin.Logging
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import ru.fix.distributed.job.manager.*
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule
import java.lang.Thread.sleep
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference


@ExperimentalStdlibApi
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@Execution(ExecutionMode.CONCURRENT)
class DJMWorkItemsBehaviourOnDisconnectsAndRestartsTest : DJMTestSuite() {
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

        val djm1 = createDJM(job)
        val djm2 = createDJM(job)
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

        val djm1 = createDJM(job1)
        val djm2 = createDJM(job2)
        val djm3 = createDJM(job3)

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

        val djm1 = createDJM(job1)
        val djm2 = createDJM(job2)
        val djm3 = createDJM(job3)

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
}

