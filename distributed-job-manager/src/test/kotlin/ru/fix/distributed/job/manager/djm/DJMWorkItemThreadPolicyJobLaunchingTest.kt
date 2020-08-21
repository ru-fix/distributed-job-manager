package ru.fix.distributed.job.manager.djm

import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import ru.fix.distributed.job.manager.*
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

class DJMWorkItemThreadPolicyJobLaunchingTest : DJMTestSuite() {
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
        await().pollDelay(100, TimeUnit.MILLISECONDS)
                .atMost(1, TimeUnit.MINUTES).until {
                    jobWithSingleThreadStrategy.receivedWorkPool.get() == workItems
                }
        closeDjm(djm)
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
        await().pollDelay(100, TimeUnit.MILLISECONDS)
                .atMost(1, TimeUnit.MINUTES).until {
                    val workShares = jobWithThreadPerWorkItem.receivedWorkShare.toList()
                    workShares.flatten().toSet().size == 10 &&
                            workShares.all { it.size == 1 }
                }

        closeDjm(djm)
    }

    @Test
    fun `single thread strategy launches all WorkItems with single run call`() {
        val job = object : DistributedJob {
            val runCalls = ConcurrentLinkedDeque<Set<String>>()

            override val jobId = JobId("job-1")
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
    fun `thread per launch strategy launches all WorkItems in different threads`() {
        val job = object : DistributedJob {
            val runCalls = ConcurrentLinkedDeque<Set<String>>()
            val threadLatch = CountDownLatch(3)

            override val jobId = JobId("job-1")
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
                    calls.flatten().groupBy { it }.all { it.value.toSet().size == 1 }

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