package ru.fix.distributed.job.manager

import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.maps.shouldContainAll
import io.kotest.matchers.maps.shouldContainExactly
import io.kotest.matchers.maps.shouldContainKey
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
@Execution(ExecutionMode.CONCURRENT)
class DistributedJobManagerRebalanceTest : DjmTestSuite() {
    companion object : Logging

    val nodeExecutedWorkItem = ConcurrentHashMap<String, String>()
    val workPool = ConcurrentHashMap.newKeySet<String>().apply {
        (1..30).forEach { add("item$it") }
    }

    inner class DynamicWorkPoolJob(val node: String)  : DistributedJob {

        override val jobId = JobId("dynamicWorkPoolJob")
        override fun getSchedule(): DynamicProperty<Schedule> = DynamicProperty.of(Schedule.withDelay(100))
        override fun run(context: DistributedJobContext) {
            for(item in context.workShare) {
                nodeExecutedWorkItem[item] = node
            }
        }
        override fun getWorkPool(): WorkPool = WorkPool.of(workPool)
        override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
        override fun getWorkPoolCheckPeriod(): Long = 50
    }

    @Test
    fun `Change in Job WorkPool triggers rebalance`() {
        for(node in 1..3) {
            createDJM(DynamicWorkPoolJob("node-$node"))
        }
        sleep(1000)

        val snapshot = HashMap(nodeExecutedWorkItem)

        workPool.add("item42")

        sleep(1000)
        nodeExecutedWorkItem.shouldContainKey("item42")
        nodeExecutedWorkItem.shouldContainAll(snapshot)
    }

    @Test
    fun `Check WorkPool period small, no change in WorkPool then should be no change to zk and no rebalance is triggered`() {
        for(node in 1..3) {
            createDJM(DynamicWorkPoolJob("node-$node"))
        }
        sleep(1000)

        val snapshot = HashMap(nodeExecutedWorkItem)

        sleep(1000)

        nodeExecutedWorkItem.shouldContainExactly(snapshot)
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

