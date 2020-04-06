package ru.fix.distributed.job.manager

import org.apache.curator.framework.CuratorFramework
import org.awaitility.Awaitility
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import ru.fix.aggregating.profiler.AggregatingProfiler
import ru.fix.distributed.job.manager.model.*
import ru.fix.distributed.job.manager.strategy.AbstractAssignmentStrategy
import ru.fix.distributed.job.manager.strategy.AssignmentStrategies
import ru.fix.distributed.job.manager.strategy.AssignmentStrategy
import ru.fix.distributed.job.manager.strategy.generateAvailability
import ru.fix.dynamic.property.api.DynamicProperty
import java.time.Duration

internal class DistributedJobManagerTest : AbstractJobManagerTest() {

    private val ussdAssignmentStrategy = object : AbstractAssignmentStrategy() {

        override fun reassignAndBalance(
                availability: MutableMap<JobId, MutableSet<WorkerId>>,
                prevAssignment: AssignmentState,
                currentAssignment: AssignmentState,
                itemsToAssign: MutableSet<WorkItem>
        ): AssignmentState {
            for ((key, value) in availability) {
                val itemsToAssignForJob = getWorkItemsByJob(key, itemsToAssign)

                value.forEach { workerId -> currentAssignment.putIfAbsent(workerId, HashSet<WorkItem>()) }

                for (item in itemsToAssignForJob) {
                    if (prevAssignment.containsWorkItem(item)) {
                        val workerFromPrevious = prevAssignment.getWorkerOfWorkItem(item)
                        currentAssignment.addWorkItem(workerFromPrevious, item)
                    } else {
                        val lessBusyWorker = currentAssignment
                                .getLessBusyWorker(value)
                        currentAssignment.addWorkItem(lessBusyWorker, item)
                    }
                }

            }
            return currentAssignment
        }
    }

    // Strategy assign work items on workers, which doesn't contains of any work item of ussd job
    private val smsAssignmentStrategy = object : AbstractAssignmentStrategy() {

        override fun reassignAndBalance(
                availability: MutableMap<JobId, MutableSet<WorkerId>>,
                prevAssignment: AssignmentState,
                currentAssignment: AssignmentState,
                itemsToAssign: MutableSet<WorkItem>
        ): AssignmentState {
            for ((key, value) in availability) {
                val itemsToAssignForJob = getWorkItemsByJob(key, itemsToAssign)
                val availableWorkers = HashSet(value)

                value.forEach { workerId ->
                    currentAssignment.putIfAbsent(workerId, HashSet<WorkItem>())

                    // ignore worker, where ussd job was launched
                    if (currentAssignment.containsAnyWorkItemOfJob(workerId, JobId("distr-job-id-1"))) {
                        availableWorkers.remove(workerId)
                    }
                }

                for (item in itemsToAssignForJob) {
                    if (currentAssignment.containsWorkItem(item)) {
                        continue
                    }

                    val lessBusyWorker = currentAssignment
                            .getLessBusyWorker(availableWorkers)
                    currentAssignment.addWorkItem(lessBusyWorker, item)
                    itemsToAssign.remove(item)
                }
            }
            return currentAssignment
        }
    }

    @Test
    @Throws(Exception::class)
    fun `evenly spread when start 3 servers with different work pools`() {
        createDjmWithEvenlySpread("worker-0", listOf(distributedJobs()[0]))
        createDjmWithEvenlySpread("worker-1", listOf(distributedJobs()[1]))
        createDjmWithEvenlySpread("worker-2", listOf(distributedJobs()[2]))
        awaitPathInit(listOf(
                paths.assignedWorkItem("worker-2", "distr-job-id-2", "distr-job-id-2.work-item-0")
        ))

        val assignedState = readAssignedState(zkTestingServer.createClient())
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
                paths.assignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-1")
        ))
        createDjmWithEvenlySpread("worker-2", distributedJobs())
        awaitPathInit(listOf(
                paths.assignedWorkItem("worker-2", "distr-job-id-1", "distr-job-id-1.work-item-5")
        ))

        val curatorFramework = zkTestingServer.createClient()
        val assignedState = readAssignedState(curatorFramework)
        assertTrue(assignedState.isBalancedForEachJob(generateAvailability(readAvailableState(curatorFramework))))
    }

    @Test
    @Throws(Exception::class)
    fun `start 3 servers with rendezvous strategy with different work pools`() {
        createDjmWithRendezvous("worker-0", listOf(distributedJobs()[0]))
        createDjmWithRendezvous("worker-1", listOf(distributedJobs()[1]))
        createDjmWithRendezvous("worker-2", listOf(distributedJobs()[2]))
        awaitPathInit(listOf(
                paths.assignedWorkItem("worker-2", "distr-job-id-2", "distr-job-id-2.work-item-0")
        ))

        val curatorFramework = zkTestingServer.createClient()
        val assignedState = readAssignedState(curatorFramework)
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
                paths.assignedWorkItem("worker-2", "distr-job-id-1", "distr-job-id-1.work-item-1")
        ))

        val nodes = listOf(
                paths.assignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-0"),
                paths.assignedWorkItem("worker-1", "distr-job-id-0", "distr-job-id-0.work-item-0"),
                paths.assignedWorkItem("worker-1", "distr-job-id-2", "distr-job-id-2.work-item-1"),

                paths.assignedWorkItem("worker-0", "distr-job-id-1", "distr-job-id-1.work-item-5"),
                paths.assignedWorkItem("worker-0", "distr-job-id-1", "distr-job-id-1.work-item-2"),
                paths.assignedWorkItem("worker-0", "distr-job-id-1", "distr-job-id-1.work-item-3"),

                paths.assignedWorkItem("worker-2", "distr-job-id-2", "distr-job-id-2.work-item-0"),
                paths.assignedWorkItem("worker-2", "distr-job-id-1", "distr-job-id-1.work-item-4"),
                paths.assignedWorkItem("worker-2", "distr-job-id-1", "distr-job-id-1.work-item-1")
        )

        val curator = zkTestingServer.createClient()
        for (node in nodes) {
            assertNotNull(curator.checkExists().forPath(node))
        }
    }

    @Test
    @Disabled("It's hard to get moment, when 3 workers alive and last third worker already have assigned paths")
    @Throws(Exception::class)
    fun `start 3 workers and destroy one of them`() {
        createDjmWithEvenlySpread("worker-0", distributedJobs())
        createDjmWithEvenlySpread("worker-1", distributedJobs())
        val destroyed = createDjmWithEvenlySpread("worker-2", distributedJobs())
        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .pollDelay(Duration.ofMillis(200))
                .until { workersAlive("worker-2", "worker-1", "worker-0") }

        val curatorFramework = zkTestingServer.createClient()
        var assignedState = readAssignedState(curatorFramework)
        assertTrue(assignedState.isBalancedForEachJob(generateAvailability(readAvailableState(curatorFramework))))

        destroyed.close()
        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .pollDelay(Duration.ofMillis(200))
                .until { readAssignedState(curatorFramework).size == 2 }

        assignedState = readAssignedState(curatorFramework)
        assertTrue(assignedState.isBalancedForEachJob(generateAvailability(readAvailableState(curatorFramework))))
    }

    @Test
    @Throws(Exception::class)
    fun `custom assigment strategy on 4 workers with identical work pool`() {
        val smsJob = StubbedMultiJob(
                0, createWorkPool("distr-job-id-0", 3).items, 50000L
        )
        val ussdJob = StubbedMultiJob(
                1, createWorkPool("distr-job-id-1", 1).items, 50000L
        )
        val rebillJob = StubbedMultiJob(
                2, createWorkPool("distr-job-id-2", 7).items, 50000L
        )

        val customStrategy = object : AbstractAssignmentStrategy() {
            override fun reassignAndBalance(
                    availability: MutableMap<JobId, MutableSet<WorkerId>>,
                    prevAssignment: AssignmentState,
                    currentAssignment: AssignmentState,
                    itemsToAssign: MutableSet<WorkItem>
            ): AssignmentState {
                var newState = ussdAssignmentStrategy.reassignAndBalance(
                        mutableMapOf(JobId("distr-job-id-1") to availability[JobId("distr-job-id-1")]!!),
                        prevAssignment,
                        currentAssignment,
                        itemsToAssign
                )
                availability.remove(JobId("distr-job-id-1"))

                newState = smsAssignmentStrategy.reassignAndBalance(
                        mutableMapOf(JobId("distr-job-id-0") to availability[JobId("distr-job-id-0")]!!),
                        prevAssignment,
                        newState,
                        itemsToAssign
                )
                availability.remove(JobId("distr-job-id-0"))

                // reassign items of other jobs using evenly spread strategy
                AssignmentStrategies.EVENLY_SPREAD.reassignAndBalance(
                        availability,
                        prevAssignment,
                        newState,
                        itemsToAssign
                )
                return newState
            }
        }

        createDjm("worker-0", listOf<DistributedJob>(smsJob, ussdJob, rebillJob), customStrategy)
        createDjm("worker-1", listOf<DistributedJob>(smsJob, ussdJob, rebillJob), customStrategy)
        createDjm("worker-2", listOf<DistributedJob>(smsJob, ussdJob, rebillJob), customStrategy)
        createDjm("worker-3", listOf<DistributedJob>(smsJob, ussdJob, rebillJob), customStrategy)
        awaitPathInit(listOf(
                paths.assignedWorkItem("worker-3", "distr-job-id-0", "distr-job-id-0.work-item-2")
        ))

        val curatorFramework = zkTestingServer.createClient()
        val assignedState = readAssignedState(curatorFramework)
        assertTrue(assignedState.isBalancedByJobId(JobId("distr-job-id-2"),
                mutableSetOf(WorkerId("worker-0"), WorkerId("worker-1"), WorkerId("worker-2"), WorkerId("worker-3")))
        )
    }

    private fun pathsExists(itemPaths: List<String>): Boolean {
        val curator = zkTestingServer.createClient()
        itemPaths.forEach {
            if (curator.checkExists().forPath(it) == null) {
                return false
            }
        }
        return true
    }

    private fun workersAlive(vararg workers: String): Boolean {
        val curator = zkTestingServer.createClient()
        workers.forEach {
            val path = ZkPathsManager(JOB_MANAGER_ZK_ROOT_PATH).aliveWorker(it)
            if (curator.checkExists().forPath(path) == null) {
                return false
            }
        }
        return true
    }

    private fun distributedJobs(): List<DistributedJob> {
        return listOf<DistributedJob>(
                StubbedMultiJob(
                        0, createWorkPool("distr-job-id-0", 1).items, 50000L
                ),
                StubbedMultiJob(
                        1, createWorkPool("distr-job-id-1", 6).items, 50000L
                ),
                StubbedMultiJob(
                        2, createWorkPool("distr-job-id-2", 2).items, 50000L
                ))
    }

    @Throws(Exception::class)
    private fun createDjm(
            nodeId: String,
            jobs: List<DistributedJob>,
            strategy: AssignmentStrategy
    ): DistributedJobManager {
        return DistributedJobManager(
                zkTestingServer.createClient(),
                jobs,
                AggregatingProfiler(),
                DistributedJobManagerSettings(
                        nodeId = nodeId,
                        rootPath = JOB_MANAGER_ZK_ROOT_PATH,
                        assignmentStrategy = strategy,
                        timeToWaitTermination = DynamicProperty.of(10_000L),
                        workPoolCleanPeriod = DynamicProperty.of(1_000L)
                )
        )
    }

    private fun readAvailableState(curatorFramework: CuratorFramework): AssignmentState {
        val availableState = AssignmentState()
        val workersRoots = curatorFramework.children
                .forPath(paths.allWorkers())

        for (worker in workersRoots) {
            if (curatorFramework.checkExists().forPath(paths.aliveWorker(worker)) == null) {
                continue
            }

            val availableJobIds = curatorFramework.children
                    .forPath(paths.availableJobs(worker))

            val availableWorkPool = HashSet<WorkItem>()
            for (availableJobId in availableJobIds) {
                val workItemsForAvailableJobList = curatorFramework.children
                        .forPath(paths.availableWorkPool(availableJobId))

                for (workItem in workItemsForAvailableJobList) {
                    availableWorkPool.add(WorkItem(workItem, JobId(availableJobId)))
                }
            }
            availableState[WorkerId(worker)] = availableWorkPool
        }
        return availableState
    }

    private fun readAssignedState(curatorFramework: CuratorFramework): AssignmentState {
        val assignedState = AssignmentState()

        val workersRoots = curatorFramework.children.forPath(paths.allWorkers())

        for (worker in workersRoots) {
            val assignedJobIds = curatorFramework.children
                    .forPath(paths.assignedJobs(worker))

            val assignedWorkPool = HashSet<WorkItem>()
            for (assignedJobId in assignedJobIds) {
                val assignedJobWorkItems = curatorFramework.children
                        .forPath(paths.assignedWorkPool(worker, assignedJobId))

                for (workItem in assignedJobWorkItems) {
                    assignedWorkPool.add(WorkItem(workItem, JobId(assignedJobId)))
                }
            }
            assignedState[WorkerId(worker)] = assignedWorkPool
        }
        return assignedState
    }

    @Throws(Exception::class)
    private fun createDjmWithEvenlySpread(nodeId: String, jobs: List<DistributedJob>): DistributedJobManager {
        return createDjm(nodeId, jobs, AssignmentStrategies.EVENLY_SPREAD)
    }

    @Throws(Exception::class)
    private fun createDjmWithRendezvous(nodeId: String, jobs: List<DistributedJob>): DistributedJobManager {
        return createDjm(nodeId, jobs, AssignmentStrategies.RENDEZVOUS)
    }

    private fun createWorkPool(jobId: String, workItemsNumber: Int): WorkPool {
        return WorkPool.of((0 until workItemsNumber)
                .map { i -> "$jobId.work-item-$i" }
                .toCollection(HashSet()))
    }

    private fun awaitPathInit(itemPaths: List<String>) {
        Awaitility.await()
                .atMost(Duration.ofSeconds(3))
                .pollDelay(Duration.ofMillis(200))
                .until { pathsExists(itemPaths) }
    }
}