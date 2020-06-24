package ru.fix.distributed.job.manager.strategy

import ru.fix.distributed.job.manager.model.AssignmentState
import ru.fix.distributed.job.manager.model.JobId
import ru.fix.distributed.job.manager.model.WorkItem
import ru.fix.distributed.job.manager.model.WorkerId
import java.util.*

class EvenlySpreadAssignmentStrategy : AbstractAssignmentStrategy() {

    override fun reassignAndBalance(
            availability: MutableMap<JobId, MutableSet<WorkerId>>,
            prevAssignment: AssignmentState,
            currentAssignment: AssignmentState,
            itemsToAssign: MutableSet<WorkItem>
    ) {
        for ((jobId, availableWorkers) in availability) {
            val itemsToAssignForJob = getWorkItemsByJob(jobId, itemsToAssign)
            availableWorkers.forEach { currentAssignment.putIfAbsent(it, HashSet<WorkItem>()) }

            for (item in itemsToAssignForJob) {
                val workerFromPrevious = prevAssignment.getWorkerOfWorkItem(item)

                if (prevAssignment.containsWorkItem(item)
                        && availableWorkers.contains(workerFromPrevious)) {
                    currentAssignment.addWorkItem(workerFromPrevious, item)
                } else {
                    val lessBusyWorker = currentAssignment
                            .getLessBusyWorkerWithJobId(jobId, availableWorkers)
                    currentAssignment.addWorkItem(lessBusyWorker, item)
                }
                itemsToAssign.remove(item)
            }

            while (!currentAssignment.isBalancedByJobId(jobId, availableWorkers)) {
                val mostBusyWorker = currentAssignment
                        .getMostBusyWorkerWithJobId(jobId, availableWorkers)
                val lessBusyWorker = currentAssignment
                        .getLessBusyWorkerWithJobId(jobId, availableWorkers)
                val itemToMove = currentAssignment.getWorkItems(mostBusyWorker, jobId).last()
                currentAssignment.moveWorkItem(itemToMove, mostBusyWorker, lessBusyWorker)
            }
        }
    }
}
