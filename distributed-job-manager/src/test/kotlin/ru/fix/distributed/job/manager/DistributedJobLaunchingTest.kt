package ru.fix.distributed.job.manager

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.string.shouldContain
import org.junit.jupiter.api.*
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import ru.fix.aggregating.profiler.NoopProfiler
import ru.fix.distributed.job.manager.model.DistributedJobManagerSettings
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule
import ru.fix.zookeeper.testing.ZKTestingServer
import java.lang.AssertionError
import java.lang.Thread.sleep
import java.util.concurrent.atomic.AtomicInteger

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@Execution(ExecutionMode.CONCURRENT)
class DistributedJobLaunchingTest {

    lateinit var server: ZKTestingServer

    @BeforeEach
    fun beforeEach() {
        server = ZKTestingServer().start()
    }

    @AfterEach
    fun afterEach() {
        server.client
    }

    @Test
    fun `job with invalid id rises an exception in DJM ctor`() {
        val invalidIdJob = object : DistributedJob {
            override fun getJobId() = JobId("little red fox")
            override fun getSchedule(): DynamicProperty<Schedule> = DynamicProperty.of(Schedule.withDelay(100))
            override fun run(context: DistributedJobContext) {
                throw AssertionError("Job does not expected to run")
            }

            override fun getWorkPool(): WorkPool = WorkPool.singleton()
            override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
            override fun getWorkPoolCheckPeriod(): Long = 0
        }

        val exc = shouldThrow<Exception> {
            DistributedJobManager(
                    server.client,
                    listOf(invalidIdJob),
                    NoopProfiler(),
                    DistributedJobManagerSettings(
                            nodeId = generateNodeId(),
                            rootPath = generateRootPath(),
                            timeToWaitTermination = DynamicProperty.of(10000)
                    ))
            Unit
        }

        exc.message.shouldContain("little red fox")
        exc.message.shouldContain("does not match")
    }

    @Test
    fun `two jobs with same id rise an exception in DJM ctor`() {
        val jobWithSameId1 = object : DistributedJob {
            override fun getJobId() = JobId("sameId")
            override fun getSchedule(): DynamicProperty<Schedule> = DynamicProperty.of(Schedule.withDelay(100))
            override fun run(context: DistributedJobContext) {
                throw AssertionError("Job does not expected to run")
            }
            override fun getWorkPool(): WorkPool = WorkPool.singleton()
            override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
            override fun getWorkPoolCheckPeriod(): Long = 0
        }

        val jobWithSameId2 = object : DistributedJob {
            override fun getJobId() = JobId("sameId")
            override fun getSchedule(): DynamicProperty<Schedule> = DynamicProperty.of(Schedule.withDelay(100))
            override fun run(context: DistributedJobContext) {
                throw AssertionError("Job does not expected to run")
            }
            override fun getWorkPool(): WorkPool = WorkPool.singleton()
            override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
            override fun getWorkPoolCheckPeriod(): Long = 0
        }

        val exc = shouldThrow<Exception> {
            DistributedJobManager(
                    server.client,
                    listOf(jobWithSameId1, jobWithSameId2),
                    NoopProfiler(),
                    DistributedJobManagerSettings(
                            nodeId = generateNodeId(),
                            rootPath = generateRootPath(),
                            timeToWaitTermination = DynamicProperty.of(10000)
                    ))
            Unit
        }

        exc.message.shouldContain("same JobId")
    }

    @Disabled
    @Test
    fun `job with invalid WorkItem behave as if there was an empty WorkItem`() {
        TODO("assert that logs contains error")
    }

    @Disabled("TODO")
    @Test
    fun `djm without any provided jobs logs a warnnig`() {
        TODO()
    }

    @Disabled("TODO")
    @Test
    fun `djm profiles how many time it took to start`() {
        TODO()
    }

    @Disabled("TODO")
    @Test
    fun `djm profiles how many time it took to shutdow`() {
        TODO()
    }

    @Disabled("TODO")
    @Test
    fun `each job launch is profiled`() {
        TODO()
    }

    @Disabled("TODO")
    @Test
    fun `all thread pools are profiled`() {
        TODO()
    }

    @Disabled("TODO")
    @Test
    fun `job restarted with delay`() {


        sleep(1000)
        TODO()
    }

    @Disabled("TODO")
    @Test
    fun `job restarted with rate`() {
        sleep(1000)
        TODO()
    }

    @Disabled("TODO")
    @Test
    fun `job restarted by schedule after failure`() {
        sleep(1000)
        TODO()
    }

    @Disabled("TODO")
    @Test
    fun `work pool single thread strategy passes several WorkItems to single job run`() {
        sleep(1000)
        TODO()
    }

    @Disabled("TODO")
    @Test
    fun `work pool thread per workItem strategy passes single WorkItem to job run and run all work items in parallel`() {
        sleep(1000)
        TODO()
    }

    @Disabled("TODO")
    @Test
    fun `custom work pool running strategy split work items between job launches`() {
        sleep(1000)
        TODO()

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

    private val lastNodeId = AtomicInteger(1)
    fun generateNodeId() = lastNodeId.incrementAndGet().toString()

    private val lastRootId = AtomicInteger(1)
    fun generateRootPath() = "root/${lastRootId.incrementAndGet()}"

}

