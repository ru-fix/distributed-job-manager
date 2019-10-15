package ru.fix.distributed.job.manager

import org.awaitility.Awaitility
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import ru.fix.aggregating.profiler.AggregatingProfiler
import ru.fix.distributed.job.manager.model.AssignmentState
import ru.fix.distributed.job.manager.model.JobId
import ru.fix.distributed.job.manager.model.WorkItem
import ru.fix.distributed.job.manager.model.WorkerId
import ru.fix.distributed.job.manager.strategy.AbstractAssignmentStrategy
import ru.fix.distributed.job.manager.strategy.AssignmentStrategies
import ru.fix.distributed.job.manager.strategy.AssignmentStrategy
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
    fun shouldEvenlyReassignWorkItemsForEachDjm() {
        createDjmWithEvenlySpread("worker-0", listOf(distributedJobs()[0]))
        createDjmWithEvenlySpread("worker-1", listOf(distributedJobs()[1]))
        createDjmWithEvenlySpread("worker-2", listOf(distributedJobs()[2]))
        awaitPathInit(listOf(
                paths.getAssignedWorkItem("worker-2", "distr-job-id-2", "distr-job-id-2.work-item-0")
        ))

        val nodes = listOf(
                paths.getAssignedWorkItem("worker-2", "distr-job-id-2", "distr-job-id-2.work-item-1"),
                paths.getAssignedWorkItem("worker-2", "distr-job-id-2", "distr-job-id-2.work-item-0"),

                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-1"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-2"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-3"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-4"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-5"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-0"),

                paths.getAssignedWorkItem("worker-0", "distr-job-id-0", "distr-job-id-0.work-item-0")
        )

        val curator = zkTestingServer.createClient()
        for (node in nodes) {
            assertNotNull(curator.checkExists().forPath(node))
        }
    }

    @Test
    @Throws(Exception::class)
    fun shouldEvenlyReassignWorkItemsForThreeIdenticalWorkers() {
        createDjmWithEvenlySpread("worker-0", distributedJobs())
        createDjmWithEvenlySpread("worker-1", distributedJobs())
        awaitPathInit(listOf(
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-1")
        ))
        createDjmWithEvenlySpread("worker-2", distributedJobs())
        awaitPathInit(listOf(
                paths.getAssignedWorkItem("worker-2", "distr-job-id-1", "distr-job-id-1.work-item-5")
        ))

        val nodes = listOf(
                paths.getAssignedWorkItem("worker-0", "distr-job-id-1", "distr-job-id-1.work-item-4"),
                paths.getAssignedWorkItem("worker-0", "distr-job-id-1", "distr-job-id-1.work-item-3"),
                paths.getAssignedWorkItem("worker-0", "distr-job-id-0", "distr-job-id-0.work-item-0"),

                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-1"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-0"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-2", "distr-job-id-2.work-item-1"),

                paths.getAssignedWorkItem("worker-2", "distr-job-id-1", "distr-job-id-1.work-item-2"),
                paths.getAssignedWorkItem("worker-2", "distr-job-id-2", "distr-job-id-2.work-item-0"),
                paths.getAssignedWorkItem("worker-2", "distr-job-id-1", "distr-job-id-1.work-item-5")
        )

        val curator = zkTestingServer.createClient()
        for (node in nodes) {
            assertNotNull(curator.checkExists().forPath(node))
        }
    }

    @Test
    @Throws(Exception::class)
    fun shouldEvenlyReassignWorkItemsForEachDjmUsingRendezvous() {
        createDjmWithRendezvous("worker-0", listOf(distributedJobs()[0]))
        createDjmWithRendezvous("worker-1", listOf(distributedJobs()[1]))
        createDjmWithRendezvous("worker-2", listOf(distributedJobs()[2]))
        awaitPathInit(listOf(
                paths.getAssignedWorkItem("worker-2", "distr-job-id-2", "distr-job-id-2.work-item-0")
        ))

        val nodes = listOf(
                paths.getAssignedWorkItem("worker-2", "distr-job-id-2", "distr-job-id-2.work-item-1"),
                paths.getAssignedWorkItem("worker-2", "distr-job-id-2", "distr-job-id-2.work-item-0"),

                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-1"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-2"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-3"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-4"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-5"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-0"),

                paths.getAssignedWorkItem("worker-0", "distr-job-id-0", "distr-job-id-0.work-item-0")
        )

        val curator = zkTestingServer.createClient()
        for (node in nodes) {
            assertNotNull(curator.checkExists().forPath(node))
        }
    }

    @Test
    @Throws(Exception::class)
    fun shouldEvenlyReassignWorkItemsForThreeIdenticalWorkersUsingRendezvous() {
        createDjmWithRendezvous("worker-0", distributedJobs())
        createDjmWithRendezvous("worker-1", distributedJobs())
        createDjmWithRendezvous("worker-2", distributedJobs())
        awaitPathInit(listOf(
                paths.getAssignedWorkItem("worker-2", "distr-job-id-1", "distr-job-id-1.work-item-1")
        ))

        val nodes = listOf(
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-0"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-0", "distr-job-id-0.work-item-0"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-2", "distr-job-id-2.work-item-1"),

                paths.getAssignedWorkItem("worker-0", "distr-job-id-1", "distr-job-id-1.work-item-5"),
                paths.getAssignedWorkItem("worker-0", "distr-job-id-1", "distr-job-id-1.work-item-2"),
                paths.getAssignedWorkItem("worker-0", "distr-job-id-1", "distr-job-id-1.work-item-3"),

                paths.getAssignedWorkItem("worker-2", "distr-job-id-2", "distr-job-id-2.work-item-0"),
                paths.getAssignedWorkItem("worker-2", "distr-job-id-1", "distr-job-id-1.work-item-4"),
                paths.getAssignedWorkItem("worker-2", "distr-job-id-1", "distr-job-id-1.work-item-1")
        )

        val curator = zkTestingServer.createClient()
        for (node in nodes) {
            assertNotNull(curator.checkExists().forPath(node))
        }
    }

    @Test
    @Throws(Exception::class)
    fun shouldEvenlyReassignIfOneWorkerDestroyed() {
        createDjmWithEvenlySpread("worker-0", distributedJobs())
        createDjmWithEvenlySpread("worker-1", distributedJobs())
        awaitPathInit(listOf(
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-0")
        ))

        val destroyed = createDjmWithEvenlySpread("worker-2", distributedJobs())
        awaitPathInit(listOf(
                paths.getAssignedWorkItem("worker-2", "distr-job-id-1", "distr-job-id-1.work-item-5")
        ))

        val nodes = listOf(
                paths.getAssignedWorkItem("worker-0", "distr-job-id-1", "distr-job-id-1.work-item-4"),
                paths.getAssignedWorkItem("worker-0", "distr-job-id-1", "distr-job-id-1.work-item-3"),
                paths.getAssignedWorkItem("worker-0", "distr-job-id-0", "distr-job-id-0.work-item-0"),

                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-1"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-0"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-2", "distr-job-id-2.work-item-1"),

                paths.getAssignedWorkItem("worker-2", "distr-job-id-1", "distr-job-id-1.work-item-2"),
                paths.getAssignedWorkItem("worker-2", "distr-job-id-2", "distr-job-id-2.work-item-0"),
                paths.getAssignedWorkItem("worker-2", "distr-job-id-1", "distr-job-id-1.work-item-5")
        )

        val curator = zkTestingServer.createClient()
        for (node in nodes) {
            assertNotNull(curator.checkExists().forPath(node))
        }

        destroyed.close()
        Awaitility.await()
                .atMost(Duration.ofSeconds(3))
                .pollDelay(Duration.ofMillis(200))
                .until { workersAlive("worker-0", "worker-1") }

        val nodesAfterDestroy = listOf(
                paths.getAssignedWorkItem("worker-0", "distr-job-id-1", "distr-job-id-1.work-item-4"),
                paths.getAssignedWorkItem("worker-0", "distr-job-id-1", "distr-job-id-1.work-item-3"),
                paths.getAssignedWorkItem("worker-0", "distr-job-id-0", "distr-job-id-0.work-item-0"),
                paths.getAssignedWorkItem("worker-0", "distr-job-id-2", "distr-job-id-2.work-item-0"),
                paths.getAssignedWorkItem("worker-0", "distr-job-id-1", "distr-job-id-1.work-item-5"),

                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-2"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-1"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-0"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-2", "distr-job-id-2.work-item-1")
        )

        for (node in nodesAfterDestroy) {
            assertNotNull(curator.checkExists().forPath(node))
        }
    }

    @Test
    @Throws(Exception::class)
    fun shouldReassignJobUsingCustomAssignmentStrategy() {
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
                paths.getAssignedWorkItem("worker-3", "distr-job-id-2", "distr-job-id-2.work-item-4")
        ))

        val nodes = listOf(
                paths.getAssignedWorkItem("worker-0", "distr-job-id-2", "distr-job-id-2.work-item-5"),
                paths.getAssignedWorkItem("worker-0", "distr-job-id-0", "distr-job-id-0.work-item-0"),
                paths.getAssignedWorkItem("worker-0", "distr-job-id-2", "distr-job-id-2.work-item-0"),

                paths.getAssignedWorkItem("worker-1", "distr-job-id-2", "distr-job-id-2.work-item-6"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-0"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-2", "distr-job-id-2.work-item-3"),

                paths.getAssignedWorkItem("worker-2", "distr-job-id-0", "distr-job-id-0.work-item-1"),
                paths.getAssignedWorkItem("worker-2", "distr-job-id-2", "distr-job-id-2.work-item-1"),
                paths.getAssignedWorkItem("worker-2", "distr-job-id-2", "distr-job-id-2.work-item-2"),

                paths.getAssignedWorkItem("worker-3", "distr-job-id-0", "distr-job-id-0.work-item-2"),
                paths.getAssignedWorkItem("worker-3", "distr-job-id-2", "distr-job-id-2.work-item-4")
        )

        val curator = zkTestingServer.createClient()
        for (node in nodes) {
            assertNotNull(curator.checkExists().forPath(node))
        }
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
            val path = JobManagerPaths(JOB_MANAGER_ZK_ROOT_PATH).getWorkerAliveFlagPath(it)
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
                nodeId,
                zkTestingServer.createClient(),
                JOB_MANAGER_ZK_ROOT_PATH,
                jobs,
                strategy,
                AggregatingProfiler(),
                DynamicProperty.of(10_000L)
        )
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