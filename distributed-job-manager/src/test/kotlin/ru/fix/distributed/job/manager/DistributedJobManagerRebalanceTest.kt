package ru.fix.distributed.job.manager

import io.kotest.matchers.booleans.shouldBeFalse
import org.apache.curator.framework.recipes.cache.CuratorCache
import org.apache.curator.framework.recipes.cache.CuratorCacheListener
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
class DistributedJobManagerRebalanceTest : DjmTestSuite() {
    companion object : Logging

    @Test
    fun `Change in Job WorkPool triggers rebalance`() {

        val dynamicWorkPoolJob = object : DistributedJob {
            val workPool = ConcurrentHashMap.newKeySet<String>().apply {
                (1..5).forEach { add("item$it") }
            }
            override val jobId = JobId("dynamicWorkPoolJob")
            override fun getSchedule(): DynamicProperty<Schedule> = DynamicProperty.of(Schedule.withDelay(100))
            override fun run(context: DistributedJobContext) {}
            override fun getWorkPool(): WorkPool = WorkPool.of(workPool)
            override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
            override fun getWorkPoolCheckPeriod(): Long = 50
        }


        val rootPath = generateDjmRootPath()

        repeat(3) { createDJM(dynamicWorkPoolJob) }

        val cache = CuratorCache.build(
                server.client,
                ZkPathsManager(rootPath).assignmentVersion())

        val assignmentVersionChanged = AtomicBoolean()

        cache.listenable().addListener(CuratorCacheListener { type, oldData, newData ->
            if(type == CuratorCacheListener.Type.NODE_CHANGED){
                assignmentVersionChanged.set(true)
            }
        })

        sleep(1000)
        assignmentVersionChanged.set(false)

        dynamicWorkPoolJob.workPool.add("item42")

        await().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(1, TimeUnit.MINUTES)
                .until { assignmentVersionChanged.get() == true }


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


}

