package ru.fix.distributed.job.manager.djm

import io.kotest.matchers.maps.shouldContainExactly
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


@ExperimentalStdlibApi
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@Execution(ExecutionMode.CONCURRENT)
class DJMRebalanceTriggeringTest : DJMTestSuite() {
    companion object : Logging

    val nodeExecutedWorkItem = ConcurrentHashMap<String, String>()
    val workPool = ConcurrentHashMap.newKeySet<String>().apply {
        (1..30).forEach { add("item$it") }
    }

    inner class DynamicWorkPoolJob(val node: String) : DistributedJob {

        override val jobId = JobId("dynamicWorkPoolJob")
        override fun getSchedule(): DynamicProperty<Schedule> = DynamicProperty.of(Schedule.withDelay(100))
        override fun run(context: DistributedJobContext) {
            for (item in context.workShare) {
                nodeExecutedWorkItem[item] = node
            }
        }

        override fun getWorkPool(): WorkPool = WorkPool.of(workPool)
        override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
        override fun getWorkPoolCheckPeriod(): Long = 50
    }

    @Test
    fun `Change in Job WorkPool triggers rebalance`() {
        for (node in 1..3) {
            createDJM(DynamicWorkPoolJob("node-$node"))
        }
        sleep(2000)
        val snapshot = HashMap(nodeExecutedWorkItem)

        workPool.add("item42")
        sleep(3000)

        await().pollInterval(100, TimeUnit.MILLISECONDS).atMost(1, TimeUnit.MINUTES).until {
            nodeExecutedWorkItem.containsKey("item42") &&
                    snapshot.keys.all {
                        nodeExecutedWorkItem.containsKey(it)
                    }
        }
    }


    @Test
    fun `Check WorkPool period small, no change in WorkPool then should be no change to zk and no rebalance is triggered`() {
        for (node in 1..3) {
            createDJM(DynamicWorkPoolJob("node-$node"))
        }
        sleep(2000)
        val snapshot = HashMap(nodeExecutedWorkItem)

        sleep(2000)
        nodeExecutedWorkItem.shouldContainExactly(snapshot)
    }

}

