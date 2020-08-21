package ru.fix.distributed.job.manager.djm

import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import org.junit.jupiter.api.Test
import ru.fix.distributed.job.manager.*
import ru.fix.distributed.job.manager.model.AssignmentState
import ru.fix.distributed.job.manager.model.WorkItem
import ru.fix.distributed.job.manager.model.WorkerId
import ru.fix.distributed.job.manager.strategy.AssignmentStrategies
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule
import java.lang.Thread.sleep
import java.util.concurrent.TimeUnit

class DJMUpdatesZkTreeAccordingToAssignmentStrategyTest : DJMTestSuite() {

    class JobForZkTreeCheck(jobId: String, private val workPool: WorkPool) : DistributedJob {
        override val jobId = JobId(jobId)
        override fun getSchedule(): DynamicProperty<Schedule> = DynamicProperty.of(Schedule.withRate(TimeUnit.HOURS.toMillis(1)))
        override fun run(context: DistributedJobContext) {}
        override fun getWorkPool(): WorkPool = workPool
        override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
        override fun getWorkPoolCheckPeriod(): Long = 0
    }

    @Test
    fun `evenly spread when start 3 servers with different job set`() {
        val job1 = JobForZkTreeCheck(
                "job-1",
                WorkPool.of("job-1-item-1", "job-1-item-2", "job-1-item-3"))
        val job2 = JobForZkTreeCheck(
                "job-2",
                WorkPool.of("job-2-item-1"))
        val job3 = JobForZkTreeCheck(
                "job-3",
                WorkPool.of("job-3-item-1", "job-3-item-2", "job-3-item-3", "job-3-item-4", "job-3-item-5"))

        createDJM(
                nodeId = "worker-1",
                jobs = listOf(job1, job3),
                assignmentStrategy = AssignmentStrategies.EVENLY_SPREAD)

        createDJM(
                nodeId = "worker-2",
                jobs = listOf(job1, job2, job3),
                assignmentStrategy = AssignmentStrategies.EVENLY_SPREAD)

        createDJM(
                nodeId = "worker-3",
                jobs = listOf(job1, job3),
                assignmentStrategy = AssignmentStrategies.EVENLY_SPREAD)

        sleep(3000)

        val assignedState = readAssignedState()
        logger.info(assignedState)
        assignedState[WorkerId("worker-1")]!!.any { it.jobId == JobId("job-2") }.shouldBeFalse()
        assignedState[WorkerId("worker-3")]!!.any { it.jobId == JobId("job-2") }.shouldBeFalse()

        assignedState.values.flatten().shouldContainExactlyInAnyOrder(
                WorkItem("job-1-item-1", JobId("job-1")),
                WorkItem("job-1-item-2", JobId("job-1")),
                WorkItem("job-1-item-3", JobId("job-1")),
                WorkItem("job-2-item-1", JobId("job-2")),
                WorkItem("job-3-item-1", JobId("job-3")),
                WorkItem("job-3-item-2", JobId("job-3")),
                WorkItem("job-3-item-3", JobId("job-3")),
                WorkItem("job-3-item-4", JobId("job-3")),
                WorkItem("job-3-item-5", JobId("job-3"))
        )
    }

    @Test
    fun `start 3 workers and destroy one of them`() {
        val job1 = JobForZkTreeCheck(
                "job-1",
                WorkPool.of("job-1-item-1", "job-1-item-2", "job-1-item-3"))
        val job2 = JobForZkTreeCheck(
                "job-2",
                WorkPool.of("job-2-item-1"))
        val job3 = JobForZkTreeCheck(
                "job-3",
                WorkPool.of("job-3-item-1", "job-3-item-2", "job-3-item-3", "job-3-item-4", "job-3-item-5"))

        createDJM(
                nodeId = "worker-1",
                jobs = listOf(job1, job2),
                assignmentStrategy = AssignmentStrategies.EVENLY_SPREAD)

        val doomedDjm = createDJM(
                nodeId = "worker-2",
                jobs = listOf(job2, job3),
                assignmentStrategy = AssignmentStrategies.EVENLY_SPREAD)

        createDJM(
                nodeId = "worker-3",
                jobs = listOf(job1, job2, job3),
                assignmentStrategy = AssignmentStrategies.EVENLY_SPREAD)

        sleep(1000)
        var assignedState = readAssignedState()
        assignedState.isBalanced.shouldBeTrue()

        closeDjm(doomedDjm)

        sleep(2000)
        assignedState = readAssignedState()
        assignedState[WorkerId("worker-1")]!!.any { it.jobId == JobId("job-3") }.shouldBeFalse()

        assignedState.values.flatten().shouldContainExactlyInAnyOrder(
                WorkItem("job-1-item-1", JobId("job-1")),
                WorkItem("job-1-item-2", JobId("job-1")),
                WorkItem("job-1-item-3", JobId("job-1")),
                WorkItem("job-2-item-1", JobId("job-2")),
                WorkItem("job-3-item-1", JobId("job-3")),
                WorkItem("job-3-item-2", JobId("job-3")),
                WorkItem("job-3-item-3", JobId("job-3")),
                WorkItem("job-3-item-4", JobId("job-3")),
                WorkItem("job-3-item-5", JobId("job-3"))
        )
    }



    private fun readAssignedState(): AssignmentState {
        val assignedState = AssignmentState()

        val workersRoots = server.client.children.forPath(djmZkPathsManager.allWorkers())

        for (worker in workersRoots) {
            val assignedJobIds = server.client.children
                    .forPath(djmZkPathsManager.assignedJobs(worker))

            val assignedWorkPool = HashSet<WorkItem>()
            for (assignedJobId in assignedJobIds) {
                val assignedJobWorkItems = server.client.children
                        .forPath(djmZkPathsManager.assignedWorkPool(worker, assignedJobId))

                for (workItem in assignedJobWorkItems) {
                    assignedWorkPool.add(WorkItem(workItem, JobId(assignedJobId)))
                }
            }
            assignedState[WorkerId(worker)] = assignedWorkPool
        }
        return assignedState
    }
}