package ru.fix.distributed.job.manager

import mu.KotlinLogging
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.imps.CuratorFrameworkState
import org.apache.zookeeper.KeeperException.NoNodeException
import ru.fix.distributed.job.manager.model.AssignmentState
import ru.fix.distributed.job.manager.model.JobId
import ru.fix.distributed.job.manager.model.WorkItem
import ru.fix.distributed.job.manager.model.WorkerId
import ru.fix.distributed.job.manager.strategy.AssignmentStrategy
import ru.fix.distributed.job.manager.util.ZkTreePrinter
import ru.fix.zookeeper.transactional.TransactionalClient
import java.util.*

private const val ASSIGNMENT_COMMIT_RETRIES_COUNT = 3

internal class Rebalancer(
        private val paths: ZkPathsManager,
        private val curatorFramework: CuratorFramework,
        private val leaderLatchExecutor: LeaderLatchExecutor,
        private val assignmentStrategy: AssignmentStrategy,
        private val nodeId: String
) : AutoCloseable {
    private val zkPrinter = ZkTreePrinter(curatorFramework)

    private val rebalanceEventReducer = EventReducer(handler = {
        leaderLatchExecutor.tryExecute(Runnable(this::reassignAndBalanceTasks))
    })

    fun handleRebalanceEvent() {
        rebalanceEventReducer.handle()
    }

    fun start() = rebalanceEventReducer.start()

    override fun close() = rebalanceEventReducer.close()

    /**
     * Rebalance tasks in tasks tree for all available workers after any failure or workers count change
     */
    private fun reassignAndBalanceTasks() {
        if (curatorFramework.state != CuratorFrameworkState.STARTED) {
            log.error("Ignore reassignAndBalanceTasks: curatorFramework is not started")
            return
        }
        if (!curatorFramework.zookeeperClient.isConnected) {
            log.error("Ignore reassignAndBalanceTasks: lost connection to zookeeper")
            return
        }
        if (log.isTraceEnabled) {
            log.trace("nodeId=$nodeId tree before rebalance: \n ${zkPrinter.print(paths.rootPath)}")
        }
        try {
            TransactionalClient.tryCommit(curatorFramework, ASSIGNMENT_COMMIT_RETRIES_COUNT) { transaction ->
                transaction.checkAndUpdateVersion(paths.assignmentVersion())
                transaction.assignWorkPools(getZookeeperGlobalState())
            }
        } catch (e: Exception) {
            log.warn("Can't reassign and balance tasks: ", e)
        }
        if (log.isTraceEnabled) {
            log.trace("nodeId=$nodeId tree after rebalance: \n ${zkPrinter.print(paths.rootPath)}")
        }
    }


    private fun TransactionalClient.assignWorkPools(globalState: GlobalAssignmentState) {
        val currentState = AssignmentState()
        val previousState = globalState.assignedState
        val availableState = globalState.availableState
        val availability = generateAvailability(availableState)

        if (log.isTraceEnabled) {
            log.trace("""
            Availability before rebalance: $availability
            Available state before rebalance: $availableState
            """.trimIndent())
        }
        val newAssignmentState: AssignmentState = assignmentStrategy.reassignAndBalance(
                availability,
                previousState,
                currentState,
                generateItemsToAssign(availableState)
        )
        if (log.isTraceEnabled) {
            log.trace("""
            Previous state before rebalance: $previousState
            New assignment after rebalance: $newAssignmentState
            """.trimIndent())
        }
        rewriteZookeeperNodes(previousState, newAssignmentState)
    }

    private fun TransactionalClient.rewriteZookeeperNodes(
            previousState: AssignmentState,
            newAssignmentState: AssignmentState
    ) {
        removeAssignmentsOnDeadNodes()
        createNodesContainedInFirstStateButNotInSecond(newAssignmentState, previousState)
        deleteNodesContainedInFirstStateButNotInSecond(previousState, newAssignmentState)
    }

    private fun TransactionalClient.createNodesContainedInFirstStateButNotInSecond(
            newAssignmentState: AssignmentState,
            previousState: AssignmentState
    ) {
        for ((workerId, workItemsOnWorker) in newAssignmentState) {
            val jobs = itemsToMap(workItemsOnWorker)
            if (curatorFramework.checkExists().forPath(paths.aliveWorker(workerId.id)) == null) {
                continue
            }
            for ((jobId, workItemsOnJob) in jobs) {
                createIfNotExist(paths.assignedWorkPool(workerId.id, jobId.id))

                for (workItem in workItemsOnJob) {
                    if (!previousState.containsWorkItemOnWorker(workerId, workItem)) {
                        createPath(paths.assignedWorkItem(workerId.id, jobId.id, workItem.id))
                    }
                }
            }
        }
    }

    private fun TransactionalClient.deleteNodesContainedInFirstStateButNotInSecond(
            previousState: AssignmentState,
            newAssignmentState: AssignmentState
    ) {
        for ((workerId, workItemsOnWorker) in previousState) {
            val jobs = itemsToMap(workItemsOnWorker)
            for ((jobId, workItemsOnJob) in jobs) {
                for (workItem in workItemsOnJob) {
                    if (!newAssignmentState.containsWorkItemOnWorker(workerId, workItem)) {
                        deletePathWithChildrenIfNeeded(paths.assignedWorkItem(workerId.id, jobId.id, workItem.id))
                    }
                }
            }
        }
    }

    private fun TransactionalClient.createIfNotExist(path: String) {
        if (curatorFramework.checkExists().forPath(path) == null) {
            createPath(path)
        }
    }

    private fun TransactionalClient.removeAssignmentsOnDeadNodes() {
        val workersRoots = getChildren(paths.allWorkers())
        for (worker in workersRoots) {
            if (curatorFramework.checkExists().forPath(paths.aliveWorker(worker)) != null) {
                continue
            }
            log.info("nodeId=$nodeId Remove dead worker $worker")
            try {
                deletePathWithChildrenIfNeeded(paths.worker(worker))
            } catch (e: NoNodeException) {
                log.info("Node was already deleted", e)
            }
        }
    }

    private fun getChildren(nodePath: String): List<String> = curatorFramework.children.forPath(nodePath)

    private fun itemsToMap(workItems: Set<WorkItem>): Map<JobId, MutableList<WorkItem>> {
        val jobs: MutableMap<JobId, MutableList<WorkItem>> = HashMap()
        for (workItem in workItems) {
            jobs.putIfAbsent(workItem.jobId, ArrayList())
            jobs[workItem.jobId]!!.add(workItem)
        }
        return jobs
    }

    private fun getZookeeperGlobalState(): GlobalAssignmentState {
        val allWorkers = curatorFramework.children.forPath(paths.allWorkers())
        val availableState = getAvailableState(allWorkers)
        val assignedState = getAssignedState(allWorkers)
        return GlobalAssignmentState(availableState, assignedState)
    }

    private fun getAssignedState(allWorkers: List<String>) = AssignmentState().also {
        for (worker in allWorkers) {
            if (curatorFramework.checkExists().forPath(paths.aliveWorker(worker)) == null) {
                continue
            }
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
            it[WorkerId(worker)] = assignedWorkPool
        }
    }

    private fun getAvailableState(allWorkers: List<String>) = AssignmentState().also {
        for (worker in allWorkers) {
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
            it[WorkerId(worker)] = availableWorkPool
        }
    }

    private fun generateAvailability(assignmentState: AssignmentState): MutableMap<JobId, MutableSet<WorkerId>> {
        val availability: MutableMap<JobId, MutableSet<WorkerId>> = HashMap()
        for ((workerId, workItems) in assignmentState) {
            for (workItem in workItems) {
                val workersOnJob = availability.computeIfAbsent(workItem.jobId) { HashSet() }
                workersOnJob.add(workerId)
            }
        }
        return availability
    }

    private fun generateItemsToAssign(assignmentState: AssignmentState): MutableSet<WorkItem> {
        val itemsToAssign = HashSet<WorkItem>()
        assignmentState.values.forEach {
            itemsToAssign.addAll(it)
        }
        return itemsToAssign
    }

    private class GlobalAssignmentState internal constructor(
            val availableState: AssignmentState,
            val assignedState: AssignmentState
    )

    companion object {
        private val log = KotlinLogging.logger {}
    }
}