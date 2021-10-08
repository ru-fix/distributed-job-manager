package ru.fix.distributed.job.manager.djm

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.ints.shouldBeInRange
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.string.shouldNotContain
import org.apache.logging.log4j.kotlin.Logging
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import ru.fix.aggregating.profiler.NoopProfiler
import ru.fix.distributed.job.manager.*
import ru.fix.distributed.job.manager.model.AssignmentState
import ru.fix.distributed.job.manager.model.Availability
import ru.fix.distributed.job.manager.model.WorkItem
import ru.fix.distributed.job.manager.strategy.AssignmentStrategy
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule
import java.lang.Thread.sleep
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.TimeUnit.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference


@TestInstance(TestInstance.Lifecycle.PER_METHOD)
//Prevent log messages from different tests to mix
@Execution(ExecutionMode.SAME_THREAD)
class DJMJobLaunchingTest : DJMTestSuite() {
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
        jobIsStarted.get().shouldBeFalse()

        logRecorder.getContent().shouldNotContain("ERROR")
        logRecorder.close()

        closeDjm(djm)
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

        closeDjm(djm)

        logRecorder.getContent().shouldContain("ERROR Failed to access job WorkPool JobId[jobWithInvalidWorkItem]")

        logRecorder.close()
    }

    @Test
    fun `job with incorrect symbols in WorkPool stops launching`() {
        val logRecorder = Log4jLogRecorder()

        val jobIsStarted = AtomicBoolean()
        val workPool = AtomicReference<String>("valid-work-pool")

        val jobWithInvalidWorkPool = object : DistributedJob {
            override val jobId = JobId("jobWithInvalidWorkPool")
            override fun getSchedule(): DynamicProperty<Schedule> = DynamicProperty.of(Schedule.withDelay(1000))
            override fun run(context: DistributedJobContext) {
                jobIsStarted.set(true)
            }

            override fun getWorkPool() = WorkPool.of(workPool.get())
            override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
            override fun getWorkPoolCheckPeriod(): Long = 100
        }

        val djm = createDJM(jobWithInvalidWorkPool)

        await().atMost(10, SECONDS).until { jobIsStarted.get() }
        workPool.set("invalid/work/pool/ы")

        sleep(1000)
        jobIsStarted.set(false)

        sleep(1000)
        jobIsStarted.get().shouldBe(false)

        closeDjm(djm)

        logRecorder.getContent().shouldContain("ERROR Failed to access job WorkPool JobId[jobWithInvalidWorkPool]")

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

        closeDjm(djm)
    }

    @Test
    fun `djm without any provided jobs logs a warning`() {
        val logRecorder = Log4jLogRecorder()
        val djm = createDJM(emptyList(), NoopProfiler())
        logRecorder.getContent().shouldContain("WARN No job instance provided")
        closeDjm(djm)
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

        closeDjm(djm)
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
                //100ms delay +/- 60ms
                it.shouldBeInRange(40..160)
            }
        jobWith100msDelay.invocationCounter.get().shouldBeInRange(8..12)
        closeDjm(djm)
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
        closeDjm(djm)
    }

    @Test
    fun `djm follows assignment strategy`() {
        val workItems = (1..10).map { it.toString() }.toSet()

        class JobForCustomAssignmnetStrategy : DistributedJob {
            val receivedWorkShare = AtomicReference<Set<String>>()
            override val jobId = JobId("jobForCustomAssignmnetStrategy")
            override fun getSchedule() = DynamicProperty.of(Schedule.withRate(100))
            override fun run(context: DistributedJobContext) {
                receivedWorkShare.set(context.workShare)
            }

            override fun getWorkPool() = WorkPool.of(workItems)
            override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
            override fun getWorkPoolCheckPeriod() = 0L
        }

        val everythingToSingleWorker = object : AssignmentStrategy {
            override fun reassignAndBalance(
                availability: Availability,
                prevAssignment: AssignmentState,
                currentAssignment: AssignmentState,
                itemsToAssign: MutableSet<WorkItem>
            ) {

                for (item in itemsToAssign) {
                    val workerId = availability[item.jobId]!!.minByOrNull { it.id }!!
                    currentAssignment.addWorkItem(workerId, item)
                }
            }
        }

        val jobs = (1..3).map { JobForCustomAssignmnetStrategy() }
        for (job in jobs) {
            createDJM(job, assignmentStrategy = everythingToSingleWorker)
        }

        await().pollInterval(100, MILLISECONDS).atMost(1, MINUTES).until {
            val receivedShares = jobs.mapNotNull { it.receivedWorkShare.get() }
            receivedShares.size == 1 &&
                    receivedShares.single() == workItems
        }
    }
}

