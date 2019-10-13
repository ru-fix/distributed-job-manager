package ru.fix.distributed.job.manager.strategy

import ru.fix.distributed.job.manager.model.AssignmentState
import ru.fix.distributed.job.manager.model.JobId
import ru.fix.distributed.job.manager.model.WorkItem
import ru.fix.distributed.job.manager.model.WorkerId

import java.util.HashSet

class EvenlySpreadAssignmentStrategy : AbstractAssignmentStrategy() {

    override fun reassignAndBalance(
            availability: Map<JobId, Set<WorkerId>>,
            prevAssignment: AssignmentState,
            currentAssignment: AssignmentState,
            itemsToAssign: MutableSet<WorkItem>
    ): AssignmentState {
        for ((jobId, availableWorkers) in availability) {

            val itemsToAssignForJob = getWorkItemsByJob(jobId, itemsToAssign)
            availableWorkers.forEach { e -> (currentAssignment as java.util.Map).putIfAbsent(e, HashSet<WorkItem>()) }

            val workersCount = availableWorkers.size
            val workItemsCount = itemsToAssignForJob.size
            val limitWorkItemsOnWorker = limitWorkItemsOnWorker(workItemsCount, workersCount)
            var itemsAssignedFromPreviousCounter = 0

            for (item in itemsToAssignForJob) {
                val workerFromPrevious = prevAssignment.getWorkerOfWorkItem(item)

                if (prevAssignment.containsWorkItem(item)
                        && currentAssignment.containsKey(workerFromPrevious)
                        && currentAssignment.isBalanced
                        && currentAssignment.isBalancedByJobId(item.jobId)
                        && itemsAssignedFromPreviousCounter < limitWorkItemsOnWorker) {
                    currentAssignment.addWorkItem(workerFromPrevious, item)
                    itemsAssignedFromPreviousCounter++

                    val limitItemsOnWorkerInPreviousState = prevAssignment
                            .get(workerFromPrevious, jobId).size
                    if (limitItemsOnWorkerInPreviousState <= itemsAssignedFromPreviousCounter) {
                        itemsAssignedFromPreviousCounter = 0
                    }
                } else {
                    val lessBusyWorker = currentAssignment
                            .getLessBusyWorkerFromAvailableWorkers(availableWorkers)
                    currentAssignment.addWorkItem(lessBusyWorker, item)
                }
                itemsToAssign.remove(item)
            }
        }
        return currentAssignment
    }

    private fun limitWorkItemsOnWorker(itemsCount: Int, workersCount: Int): Int {
        return itemsCount / workersCount + if (itemsCount % workersCount == 0) 0 else 1
    }

}
