package ru.fix.distributed.job.manager

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.imps.CuratorFrameworkState
import org.apache.logging.log4j.kotlin.Logging
import org.apache.zookeeper.KeeperException.NoNodeException
import ru.fix.distributed.job.manager.model.*
import ru.fix.distributed.job.manager.strategy.AssignmentStrategy
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.zookeeper.transactional.ZkTransaction
import ru.fix.zookeeper.utils.ZkTreePrinter
import java.util.*

private const val ASSIGNMENT_COMMIT_RETRIES_COUNT = 3

/**
 *  Performs rebalance according to ZK tree state, using given [assignmentStrategy]
 * */
internal class Rebalancer(
    private val paths: ZkPathsManager,
    private val curatorFramework: CuratorFramework,
    private val assignmentStrategy: AssignmentStrategy,
    private val nodeId: String,
    private val disableConfigProperty: DynamicProperty<JobDisableConfig>
) {
    private val zkPrinter = ZkTreePrinter(curatorFramework)

    /**
     * rebalance current available work-items between current alive workers using given [assignmentStrategy]
     */
    fun reassignAndBalanceTasks() {
        logger.info("Rebalance nodes by DJM nodeId=$nodeId")
        if (curatorFramework.state != CuratorFrameworkState.STARTED) {
            logger.error("Ignore reassignAndBalanceTasks: curatorFramework is not started")
            return
        }
        if (!curatorFramework.zookeeperClient.isConnected) {
            logger.error("Ignore reassignAndBalanceTasks: lost connection to zookeeper")
            return
        }
        logger.trace { "nodeId=$nodeId tree before rebalance: \n ${zkPrinter.print(paths.rootPath)}" }
        try {
            ZkTransaction.tryCommit(curatorFramework, ASSIGNMENT_COMMIT_RETRIES_COUNT) { transaction ->
                transaction.readVersionThenCheckAndUpdateInTransactionIfItMutatesZkState(paths.assignmentVersion())
                transaction.assignWorkPools(getZookeeperGlobalState())
            }
        } catch (e: Exception) {
            logger.warn("Can't reassign and balance tasks: ", e)
        }
        logger.trace { "nodeId=$nodeId tree after rebalance: \n ${zkPrinter.print(paths.rootPath)}" }
    }


    private fun ZkTransaction.assignWorkPools(globalState: GlobalAssignmentState) {
        val newState = AssignmentState()
        val previousState = globalState.assignedState
        val availableState = globalState.availableState
        val availability = generateAvailability(availableState)

        logger.trace {
            """
            Availability before rebalance: $availability
            Available state before rebalance: $availableState
            """.trimIndent()
        }

        assignmentStrategy.reassignAndBalance(
            availability,
            previousState,
            newState,
            generateItemsToAssign(availableState)
        )

        logger.trace {
            """
            Previous state before rebalance: $previousState
            New assignment after rebalance: $newState
            """.trimIndent()
        }

        rewriteZookeeperNodes(previousState, newState)
    }

    private fun ZkTransaction.rewriteZookeeperNodes(
        previousState: AssignmentState,
        newAssignmentState: AssignmentState
    ) {
        removeAssignmentsOnDeadNodes()
        createNodesContainedInFirstStateButNotInSecond(newAssignmentState, previousState)
        deleteNodesContainedInFirstStateButNotInSecond(previousState, newAssignmentState)
    }

    private fun ZkTransaction.createNodesContainedInFirstStateButNotInSecond(
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

    private fun ZkTransaction.deleteNodesContainedInFirstStateButNotInSecond(
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

    private fun ZkTransaction.createIfNotExist(path: String) {
        if (curatorFramework.checkExists().forPath(path) == null) {
            createPath(path)
        }
    }

    private fun ZkTransaction.removeAssignmentsOnDeadNodes() {
        val workersRoots = getChildren(paths.allWorkers())
        for (worker in workersRoots) {
            if (curatorFramework.checkExists().forPath(paths.aliveWorker(worker)) != null) {
                continue
            }
            logger.info { "nodeId=$nodeId Remove dead worker $worker" }
            try {
                deletePathWithChildrenIfNeeded(paths.worker(worker))
            } catch (e: NoNodeException) {
                logger.info("Node was already deleted", e)
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

    private fun getAvailableState(allWorkers: List<String>) = AssignmentState().also { state ->
        for (worker in allWorkers) {
            if (curatorFramework.checkExists().forPath(paths.aliveWorker(worker)) == null) {
                continue
            }
            val disableConfig = disableConfigProperty.get()
            val availableJobIds = curatorFramework.children
                .forPath(paths.availableJobs(worker))
                .filter { disableConfig.isJobShouldBeLaunched(it) }

            val availableWorkPool = HashSet<WorkItem>()
            for (availableJobId in availableJobIds) {
                val workItemsForAvailableJobList = curatorFramework.children
                    .forPath(paths.availableWorkPool(availableJobId))
                for (workItem in workItemsForAvailableJobList) {
                    availableWorkPool.add(WorkItem(workItem, JobId(availableJobId)))
                }
            }
            state[WorkerId(worker)] = availableWorkPool
        }
    }

    private fun generateAvailability(assignmentState: AssignmentState): Availability {
        val availability = Availability()
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

    companion object : Logging
}