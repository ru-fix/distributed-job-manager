package ru.fix.distributed.job.manager.djm

import org.awaitility.Awaitility
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import ru.fix.distributed.job.manager.*
import ru.fix.distributed.job.manager.model.AssignmentState
import ru.fix.distributed.job.manager.model.Availability
import ru.fix.distributed.job.manager.model.WorkItem
import ru.fix.distributed.job.manager.model.WorkerId
import ru.fix.distributed.job.manager.strategy.AbstractAssignmentStrategy
import ru.fix.distributed.job.manager.strategy.AssignmentStrategies
import ru.fix.distributed.job.manager.strategy.generateAvailability
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule
import java.time.Duration
import java.util.concurrent.TimeUnit

class DJMUpdatesZkTreeAccordingToAssignmentStrategyTest : DJMTestSuite() {

    @Test
    fun `evenly spread when start 3 servers with different work pools`() {
        createDjmWithEvenlySpread("worker-0", listOf(distributedJobs()[0]))
        createDjmWithEvenlySpread("worker-1", listOf(distributedJobs()[1]))
        createDjmWithEvenlySpread("worker-2", listOf(distributedJobs()[2]))

        awaitPathInit(listOf(
                djmZkPathsManager.assignedWorkItem("worker-2", "distr-job-id-2", "distr-job-id-2.work-item-0")
        ))

        val assignedState = readAssignedState()
        assertTrue(assignedState.isBalancedByJobId(JobId("distr-job-id-0"), mutableSetOf(WorkerId("worker-0"))))
        assertTrue(assignedState.isBalancedByJobId(JobId("distr-job-id-1"), mutableSetOf(WorkerId("worker-1"))))
        assertTrue(assignedState.isBalancedByJobId(JobId("distr-job-id-2"), mutableSetOf(WorkerId("worker-2"))))
    }

    @Test
    @Throws(Exception::class)
    fun `evenly spread when start 3 servers with identical work pool`() {
        createDjmWithEvenlySpread("worker-0", distributedJobs())
        createDjmWithEvenlySpread("worker-1", distributedJobs())
        awaitPathInit(listOf(
                djmZkPathsManager.assignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-1")
        ))
        createDjmWithEvenlySpread("worker-2", distributedJobs())
        awaitPathInit(listOf(
                djmZkPathsManager.assignedWorkItem("worker-2", "distr-job-id-1", "distr-job-id-1.work-item-5")
        ))

        val assignedState = readAssignedState()
        assertTrue(assignedState.isBalancedForEachJob(generateAvailability(readAvailableState())))
    }

    @Test
    @Throws(Exception::class)
    fun `start 3 servers with rendezvous strategy with different work pools`() {
        createDjmWithRendezvous("worker-0", listOf(distributedJobs()[0]))
        createDjmWithRendezvous("worker-1", listOf(distributedJobs()[1]))
        createDjmWithRendezvous("worker-2", listOf(distributedJobs()[2]))
        awaitPathInit(listOf(
                djmZkPathsManager.assignedWorkItem("worker-2", "distr-job-id-2", "distr-job-id-2.work-item-0")
        ))

        val assignedState = readAssignedState()
        assertTrue(assignedState.isBalancedByJobId(JobId("distr-job-id-0"), mutableSetOf(WorkerId("worker-0"))))
        assertTrue(assignedState.isBalancedByJobId(JobId("distr-job-id-1"), mutableSetOf(WorkerId("worker-1"))))
        assertTrue(assignedState.isBalancedByJobId(JobId("distr-job-id-2"), mutableSetOf(WorkerId("worker-2"))))
    }

    @Test
    @Throws(Exception::class)
    fun `start 3 servers with rendezvous strategy with identical work pool`() {
        createDjmWithRendezvous("worker-0", distributedJobs())
        createDjmWithRendezvous("worker-1", distributedJobs())
        createDjmWithRendezvous("worker-2", distributedJobs())
        awaitPathInit(listOf(
                djmZkPathsManager.assignedWorkItem("worker-2", "distr-job-id-1", "distr-job-id-1.work-item-1")
        ))

        val nodes = listOf(
                djmZkPathsManager.assignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-0"),
                djmZkPathsManager.assignedWorkItem("worker-1", "distr-job-id-0", "distr-job-id-0.work-item-0"),
                djmZkPathsManager.assignedWorkItem("worker-1", "distr-job-id-2", "distr-job-id-2.work-item-1"),

                djmZkPathsManager.assignedWorkItem("worker-0", "distr-job-id-1", "distr-job-id-1.work-item-5"),
                djmZkPathsManager.assignedWorkItem("worker-0", "distr-job-id-1", "distr-job-id-1.work-item-2"),
                djmZkPathsManager.assignedWorkItem("worker-0", "distr-job-id-1", "distr-job-id-1.work-item-3"),

                djmZkPathsManager.assignedWorkItem("worker-2", "distr-job-id-2", "distr-job-id-2.work-item-0"),
                djmZkPathsManager.assignedWorkItem("worker-2", "distr-job-id-1", "distr-job-id-1.work-item-4"),
                djmZkPathsManager.assignedWorkItem("worker-2", "distr-job-id-1", "distr-job-id-1.work-item-1")
        )

        for (node in nodes) {
            assertNotNull(server.client.checkExists().forPath(node))
        }
    }

    @Test
    fun `start 3 workers and destroy one of them`() {
        createDjmWithEvenlySpread("worker-0", distributedJobs())
        createDjmWithEvenlySpread("worker-1", distributedJobs())
        val doomedDjm = createDjmWithEvenlySpread("worker-2", distributedJobs())
        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .pollDelay(Duration.ofMillis(200))
                .until { workersAlive("worker-2", "worker-1", "worker-0") }

        var assignedState = readAssignedState()
        assertTrue(assignedState.isBalancedForEachJob(generateAvailability(readAvailableState())))

        closeDjm(doomedDjm)
        Awaitility.await()
                .atMost(Duration.ofSeconds(20))
                .pollDelay(Duration.ofMillis(200))
                .until { readAssignedState().size == 2 }

        assignedState = readAssignedState()
        assertTrue(assignedState.isBalancedForEachJob(generateAvailability(readAvailableState())))
    }


    private fun pathsExists(itemPaths: List<String>): Boolean {
        val curator = server.client
        itemPaths.forEach {
            if (curator.checkExists().forPath(it) == null) {
                return false
            }
        }
        return true
    }

    private fun workersAlive(vararg workers: String): Boolean {
        workers.forEach {
            val path = djmZkPathsManager.aliveWorker(it)
            if (server.client.checkExists().forPath(path) == null) {
                return false
            }
        }
        return true
    }

    class JobWithBigDelay(identity: Int, private val workPool: WorkPool): DistributedJob{
        override val jobId = JobId("distr-job-id-" + identity)
        override fun getSchedule(): DynamicProperty<Schedule> = DynamicProperty.of(Schedule.withRate(TimeUnit.HOURS.toMillis(1)))
        override fun run(context: DistributedJobContext) { }
        override fun getWorkPool(): WorkPool = workPool
        override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
        override fun getWorkPoolCheckPeriod(): Long = 0
    }


    private fun distributedJobs(): List<DistributedJob> {
        return listOf<DistributedJob>(
                JobWithBigDelay(0, createWorkPool("distr-job-id-0", 1)),
                JobWithBigDelay(1, createWorkPool("distr-job-id-1", 6)),
                JobWithBigDelay(2, createWorkPool("distr-job-id-2", 2))
        )
    }

    private fun readAvailableState(): AssignmentState {
        val curatorFramework = server.client
        val availableState = AssignmentState()
        val workersRoots = curatorFramework.children
                .forPath(djmZkPathsManager.allWorkers())

        for (worker in workersRoots) {
            if (curatorFramework.checkExists().forPath(djmZkPathsManager.aliveWorker(worker)) == null) {
                continue
            }

            val availableJobIds = curatorFramework.children
                    .forPath(djmZkPathsManager.availableJobs(worker))

            val availableWorkPool = HashSet<WorkItem>()
            for (availableJobId in availableJobIds) {
                val workItemsForAvailableJobList = curatorFramework.children
                        .forPath(djmZkPathsManager.availableWorkPool(availableJobId))

                for (workItem in workItemsForAvailableJobList) {
                    availableWorkPool.add(WorkItem(workItem, JobId(availableJobId)))
                }
            }
            availableState[WorkerId(worker)] = availableWorkPool
        }
        return availableState
    }

    private fun readAssignedState(): AssignmentState {
        val curatorFramework = server.client
        val assignedState = AssignmentState()

        val workersRoots = curatorFramework.children.forPath(djmZkPathsManager.allWorkers())

        for (worker in workersRoots) {
            val assignedJobIds = curatorFramework.children
                    .forPath(djmZkPathsManager.assignedJobs(worker))

            val assignedWorkPool = HashSet<WorkItem>()
            for (assignedJobId in assignedJobIds) {
                val assignedJobWorkItems = curatorFramework.children
                        .forPath(djmZkPathsManager.assignedWorkPool(worker, assignedJobId))

                for (workItem in assignedJobWorkItems) {
                    assignedWorkPool.add(WorkItem(workItem, JobId(assignedJobId)))
                }
            }
            assignedState[WorkerId(worker)] = assignedWorkPool
        }
        return assignedState
    }

    private fun createDjmWithEvenlySpread(nodeId: String, jobs: List<DistributedJob>): DistributedJobManager {
        return createDJM(nodeId = nodeId, jobs = jobs, assignmentStrategy = AssignmentStrategies.EVENLY_SPREAD)
    }

    private fun createDjmWithRendezvous(nodeId: String, jobs: List<DistributedJob>): DistributedJobManager {
        return createDJM(nodeId = nodeId, jobs = jobs, assignmentStrategy = AssignmentStrategies.RENDEZVOUS)
    }

    private fun createWorkPool(jobId: String, workItemsNumber: Int): WorkPool {
        return WorkPool.of((0 until workItemsNumber)
                .map { i -> "$jobId.work-item-$i" }
                .toSet())
    }

    private fun awaitPathInit(itemPaths: List<String>) {
        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .pollDelay(Duration.ofMillis(200))
                .until { pathsExists(itemPaths) }
    }
}