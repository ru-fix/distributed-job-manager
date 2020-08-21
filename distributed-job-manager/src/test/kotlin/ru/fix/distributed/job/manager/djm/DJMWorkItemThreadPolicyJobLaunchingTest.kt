package ru.fix.distributed.job.manager.djm

import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import ru.fix.distributed.job.manager.*
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class DJMWorkItemThreadPolicyJobLaunchingTest : DJMTestSuite() {


    @Test
    fun `single thread strategy launches all WorkItems with single run call`() {
        val job = object : DistributedJob {
            val runCalls = ConcurrentLinkedDeque<Set<String>>()

            override val jobId = JobId("job-with-single-thread-strategy")
            override fun getSchedule() = DynamicProperty.of(Schedule.withDelay(100))
            override fun run(context: DistributedJobContext) {
                runCalls.add(context.workShare)
            }

            override fun getWorkPool() = WorkPool.of("item-1", "item-2", "item-3")
            override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
            override fun getWorkPoolCheckPeriod() = 0L
        }
        createDJM(job)

        await().until {
            job.runCalls.size > 3 &&
                    job.runCalls.all { it.size == 3 }

        }
    }

    @Test
    fun `thread per workItem strategy passes single WorkItem to job run and run all items in parallel`() {
        val job = object : DistributedJob {
            val runCalls = ConcurrentLinkedDeque<Set<String>>()
            val threadLatch = CountDownLatch(3)

            override val jobId = JobId("job-with-thread-per-work-item")
            override fun getSchedule() = DynamicProperty.of(Schedule.withDelay(100))
            override fun run(context: DistributedJobContext) {
                threadLatch.countDown()
                threadLatch.await()
                runCalls.add(context.workShare)
            }

            override fun getWorkPool() = WorkPool.of("item-1", "item-2", "item-3")
            override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getThreadPerWorkItemStrategy()
            override fun getWorkPoolCheckPeriod() = 0L
        }
        createDJM(job)

        await().atMost(30, TimeUnit.SECONDS).until {
            val calls = job.runCalls.toList()
            calls.size > 3 * 3 &&
                    calls.all { it.size == 1 } &&
                    calls.flatten().groupBy { it }.all { it.value.toSet().size == 1 } &&
                    calls.flatten().groupBy { it }.keys.toSet() == setOf("item-1", "item-2", "item-3")

        }
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
        await().pollDelay(100, TimeUnit.MILLISECONDS)
                .atMost(1, TimeUnit.MINUTES).until {
                    val workShares = jobWithCustomWorkPoolRunningnStrategy.receivedWorkShare.toList()
                    workShares.flatten().toSet().size == 10 &&
                            workShares.all { it.size == 2 }
                }

        closeDjm(djm)
    }
}