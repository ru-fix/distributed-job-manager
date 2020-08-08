package ru.fix.distributed.job.manager

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.ints.shouldBeInRange
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.string.shouldNotContain
import org.apache.logging.log4j.kotlin.Logging
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.*
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import ru.fix.aggregating.profiler.NoopProfiler
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule
import java.lang.IllegalStateException
import java.lang.Thread.sleep
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.TimeUnit.*
import java.util.concurrent.atomic.*


@TestInstance(TestInstance.Lifecycle.PER_METHOD)
//Prevent log messages from different tests to mix
@Execution(ExecutionMode.SAME_THREAD)
class DistributedJobLaunchingTest : DjmTestSuite() {
    companion object : Logging

    @Test
    fun `job with invalid id rises an exception during it's object creation`() {
        val exc = shouldThrow<Exception> {
            object : DistributedJob {
                override val jobId = JobId("little red fox")
                override fun getSchedule(): DynamicProperty<Schedule> = DynamicProperty.of(Schedule.withDelay(100))
                override fun run(context: DistributedJobContext) {
                    throw AssertionError("Job does not expected to run")
                }

                override fun getWorkPool(): WorkPool = WorkPool.singleton()
                override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
                override fun getWorkPoolCheckPeriod(): Long = 0
            }
            Unit
        }

        exc.message.shouldContain("little red fox")
        exc.message.shouldContain("does not match")
    }

    @Test
    fun `two jobs with same id rise an exception in DJM ctor`() {
        val jobWithSameId1 = object : DistributedJob {
            override val jobId = JobId("sameId")
            override fun getSchedule(): DynamicProperty<Schedule> = DynamicProperty.of(Schedule.withDelay(100))
            override fun run(context: DistributedJobContext) {
                throw AssertionError("Job does not expected to run")
            }

            override fun getWorkPool(): WorkPool = WorkPool.singleton()
            override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
            override fun getWorkPoolCheckPeriod(): Long = 0
        }

        val jobWithSameId2 = object : DistributedJob {
            override val jobId = JobId("sameId")
            override fun getSchedule(): DynamicProperty<Schedule> = DynamicProperty.of(Schedule.withDelay(100))
            override fun run(context: DistributedJobContext) {
                throw AssertionError("Job does not expected to run")
            }

            override fun getWorkPool(): WorkPool = WorkPool.singleton()
            override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
            override fun getWorkPoolCheckPeriod(): Long = 0
        }

        val exc = shouldThrow<Exception> {
            createDJM(jobs = listOf(jobWithSameId1, jobWithSameId2), profiler = NoopProfiler())
        }

        exc.message.shouldContain("same JobId")
    }

    @Test
    fun `job with empty WorkPool list does not starts `() {
        val logRecorder = Log4jLogRecorder()

        val jobIsStarted = AtomicBoolean()
        val jobWithEmptyWorkPool = object : DistributedJob {
            override val jobId = JobId("jobWithEmptyWorkPool")
            override fun getSchedule(): DynamicProperty<Schedule> = DynamicProperty.of(Schedule.withDelay(10))
            override fun run(context: DistributedJobContext) {
                jobIsStarted.set(true)
            }

            override fun getWorkPool(): WorkPool = WorkPool(emptySet())
            override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
            override fun getWorkPoolCheckPeriod(): Long = 100
        }
        val djm = createDJM(jobWithEmptyWorkPool)

        sleep(3000)
        jobIsStarted.get().shouldBe(false)

        djm.close()

        logRecorder.getContent().shouldNotContain("ERROR")
        logRecorder.close()
    }


    @Test
    fun `after failed getWorkPool job stop launching`() {
        val logRecorder = Log4jLogRecorder()

        val jobIsStarted = AtomicBoolean()
        val damageWorkPool = AtomicBoolean()

        val jobWithInvalidWorkItem = object : DistributedJob {
            override val jobId = JobId("jobWithInvalidWorkItem")
            override fun getSchedule(): DynamicProperty<Schedule> = DynamicProperty.of(Schedule.withDelay(1000))
            override fun run(context: DistributedJobContext) {
                jobIsStarted.set(true)
            }

            override fun getWorkPool(): WorkPool {
                if (damageWorkPool.get()) {
                    throw Exception("Failed to calculate workpool")
                } else {
                    return WorkPool.singleton()
                }
            }

            override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
            override fun getWorkPoolCheckPeriod(): Long = 100
        }

        val djm = createDJM(jobWithInvalidWorkItem)

        await().atMost(10, SECONDS).until { jobIsStarted.get() }
        damageWorkPool.set(true)

        sleep(1000)
        jobIsStarted.set(false)

        sleep(1000)
        jobIsStarted.get().shouldBe(false)

        djm.close()

        logRecorder.getContent().shouldContain("ERROR Failed to access job WorkPool JobId[jobWithInvalidWorkItem]")

        logRecorder.close()
    }

    @Test
    fun `when work pool changes, new work share passed to job launch context`() {
        val jobReceivedWorkPool = AtomicReference<Set<String>>()
        val workPool = AtomicReference<Set<String>>(setOf("work-item-1"))

        val jobWithDynamicWorkPool = object : DistributedJob {
            override val jobId = JobId("jobWithDynamicWorkPool")
            override fun getSchedule(): DynamicProperty<Schedule> = DynamicProperty.of(Schedule.withDelay(1000))
            override fun run(context: DistributedJobContext) {
                jobReceivedWorkPool.set(context.workShare)
            }

            override fun getWorkPool(): WorkPool {
                return WorkPool.of(workPool.get())
            }

            override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
            override fun getWorkPoolCheckPeriod(): Long = 50
        }
        val djm = createDJM(jobWithDynamicWorkPool)

        await().atMost(10, SECONDS).until { jobReceivedWorkPool.get() == setOf("work-item-1") }

        workPool.set(setOf("work-item-2"))

        await().atMost(10, SECONDS).until { jobReceivedWorkPool.get() == setOf("work-item-2") }

        djm.close()
    }

    @Test
    fun `djm without any provided jobs logs a warning`() {
        val logRecorder = Log4jLogRecorder()
        val djm = createDJM(emptyList(), NoopProfiler())
        logRecorder.getContent().shouldContain("WARN No job instance provided")
        djm.close()
        logRecorder.close()
    }

    @Test
    fun `after faliure job logs error details and succesfully restarts`() {
        val jobWithFailedFirstInvocation = object : DistributedJob {
            val invocationCounter = AtomicInteger()
            override val jobId = JobId("jobWithFailedFirstInvocation")
            override fun getSchedule(): DynamicProperty<Schedule> = DynamicProperty.of(Schedule.withDelay(10))
            override fun run(context: DistributedJobContext) {
                val invocationNumber = invocationCounter.incrementAndGet()
                if (invocationNumber == 1) {
                    throw IllegalStateException("First failed invocation")
                }
            }

            override fun getWorkPool() = WorkPool.singleton()
            override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
            override fun getWorkPoolCheckPeriod(): Long = 0
        }

        val logRecorder = Log4jLogRecorder()

        val djm = createDJM(jobWithFailedFirstInvocation)
        await().atMost(1, MINUTES).until {
            jobWithFailedFirstInvocation.invocationCounter.get() > 10
        }

        logRecorder.getContent().contains("First failed invocation")

        djm.close()
        logRecorder.close()
    }

    @Test
    fun `job restarted with delay`() {
        val jobWith100msDelay = object : DistributedJob {
            val previousStartTime = AtomicReference(Instant.now())
            val delaysPerInvocation = ConcurrentLinkedDeque<Duration>()
            val invocationCounter = AtomicInteger()

            override val jobId = JobId("jobWith100msDelay")
            override fun getSchedule(): DynamicProperty<Schedule> = DynamicProperty.of(Schedule.withDelay(100))
            override fun run(context: DistributedJobContext) {
                val now = Instant.now()
                invocationCounter.incrementAndGet()
                delaysPerInvocation.add(Duration.between(previousStartTime.get(), now))
                previousStartTime.set(now)
            }

            override fun getWorkPool() = WorkPool.singleton()
            override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
            override fun getWorkPoolCheckPeriod(): Long = 0
        }
        val djm = createDJM(jobWith100msDelay)
        await().pollDelay(10, MILLISECONDS)
                .atMost(1, MINUTES)
                .until { jobWith100msDelay.invocationCounter.get() > 10 }

        jobWith100msDelay.invocationCounter.set(0)
        sleep(1000)

        jobWith100msDelay.delaysPerInvocation.toList()
                .drop(10)
                .map { it.toMillis().toInt() }
                .forEach {
                    //100ms delay +/- 20ms
                    it.shouldBeInRange(80..120)
                }
        jobWith100msDelay.invocationCounter.get().shouldBeInRange(8..12)
        djm.close()
    }

    @Test
    fun `job restarted with rate`() {
        val jobWithRate100ms = object : DistributedJob {
            val invocationCounter = AtomicInteger()

            override val jobId = JobId("jobWithRate100ms")
            override fun getSchedule(): DynamicProperty<Schedule> = DynamicProperty.of(Schedule.withRate(100))
            override fun run(context: DistributedJobContext) {
                invocationCounter.incrementAndGet()
            }

            override fun getWorkPool() = WorkPool.singleton()
            override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
            override fun getWorkPoolCheckPeriod(): Long = 0
        }
        val djm = createDJM(jobWithRate100ms)
        await().pollDelay(10, MILLISECONDS)
                .atMost(1, MINUTES).until { jobWithRate100ms.invocationCounter.get() > 10 }

        jobWithRate100ms.invocationCounter.set(0)
        sleep(1000)
        jobWithRate100ms.invocationCounter.get().shouldBeInRange(8..12)
        djm.close()
    }


    @Test
    fun `work pool single thread strategy passes several WorkItems to single job run`() {
        val workItems = (1..10).map { it.toString() }.toSet()

        val jobWithSingleThreadStrategy = object : DistributedJob {
            val receivedWorkPool = AtomicReference<Set<String>>()

            override val jobId = JobId("jobWithSingleThreadStrategy")
            override fun getSchedule(): DynamicProperty<Schedule> = DynamicProperty.of(Schedule.withRate(10))
            override fun run(context: DistributedJobContext) {
                receivedWorkPool.set(context.workShare)
            }

            override fun getWorkPool() = WorkPool.of(workItems)
            override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
            override fun getWorkPoolCheckPeriod(): Long = 0
        }
        val djm = createDJM(jobWithSingleThreadStrategy)
        await().pollDelay(100, MILLISECONDS)
                .atMost(1, MINUTES).until {
                    jobWithSingleThreadStrategy.receivedWorkPool.get() == workItems
                }
        djm.close()
    }

    @Test
    fun `work pool thread per workItem strategy passes single WorkItem to job run and run all work items in parallel`() {
        val workItems = (1..10).map { it.toString() }.toSet()

        val jobWithThreadPerWorkItem = object : DistributedJob {
            val receivedWorkShare = ConcurrentLinkedDeque<Set<String>>()

            override val jobId = JobId("jobWithThreadPerWorkItem")
            override fun getSchedule() = DynamicProperty.of(Schedule.withRate(100))
            override fun run(context: DistributedJobContext) {
                receivedWorkShare.add(context.workShare)
            }

            override fun getWorkPool() = WorkPool.of(workItems)
            override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getThreadPerWorkItemStrategy()
            override fun getWorkPoolCheckPeriod(): Long = 0
        }

        val djm = createDJM(jobWithThreadPerWorkItem)
        await().pollDelay(100, MILLISECONDS)
                .atMost(1, MINUTES).until {
                    val workShares = jobWithThreadPerWorkItem.receivedWorkShare.toList()
                    workShares.flatten().toSet().size == 10 &&
                            workShares.all { it.size == 1 }
                }

        djm.close()
    }

    @Test
    fun `custom work pool running strategy split work items between job launches`() {
        val workItems = (1..10).map { it.toString() }.toSet()

        val jobWithCustomWorkPoolRunningnStrategy = object : DistributedJob {
            val receivedWorkShare = ConcurrentLinkedDeque<Set<String>>()

            override val jobId = JobId("jobWithCustomWorkPoolRunningnStrategy")
            override fun getSchedule() = DynamicProperty.of(Schedule.withRate(100))
            override fun run(context: DistributedJobContext) {
                receivedWorkShare.add(context.workShare)
            }
            override fun getWorkPool() = WorkPool.of(workItems)
            override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategy { workPool -> workPool.size / 2 }
            override fun getWorkPoolCheckPeriod() = 0L
        }

        val djm = createDJM(jobWithCustomWorkPoolRunningnStrategy)
        await().pollDelay(100, MILLISECONDS)
                .atMost(1, MINUTES).until {
                    val workShares = jobWithCustomWorkPoolRunningnStrategy.receivedWorkShare.toList()
                    workShares.flatten().toSet().size == 10 &&
                            workShares.all { it.size == 2 }
                }

        djm.close()
    }

    @Disabled("TODO")
    @Test
    fun `during disconnect of DJM1, DJM2 does not steal WorkItem that currently under work by DJM1`() {
        sleep(1000)
        TODO()

    }

    @Disabled("TODO")
    @Test
    fun `when DJM3 disconnects, WorkItems rebalanced between DJM1 and DJM2`() {
        sleep(1000)
        TODO()
    }

    @Disabled("TODO")
    @Test
    fun `when DJM3 shutdowns, WorkItems rebalanced between DJM1 and DJM2`() {
        sleep(1000)
        TODO()
    }

    @Disabled("TODO")
    @Test
    fun `series of random DJMs disconnects, shutdowns, launches, WorkPool changes and restarts does not affect correct WorkItem launching and schedulling`() {
        sleep(1000)
        TODO("same workItem running only by single thread within the cluster")
        TODO("all workItem runs by schedule as expected with small disturbances")

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

