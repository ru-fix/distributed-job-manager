package ru.fix.distributed.job.manager.strategy

import ru.fix.distributed.job.manager.model.AssignmentState
import ru.fix.distributed.job.manager.model.JobId
import ru.fix.distributed.job.manager.model.WorkItem
import ru.fix.distributed.job.manager.model.WorkerId
import java.util.*
import java.util.stream.Collectors
import java.util.stream.IntStream

fun addWorkerWithItems(state: AssignmentState, worker: String, workItemsCount: Int, jobsCount: Int) {
    val workItems = HashSet<WorkItem>()

    for (i in 0 until workItemsCount) {
        for (j in 0 until jobsCount) {
            workItems.add(WorkItem("work-item-$i", JobId("job-$j")))
        }
    }
    state.addWorkItems(WorkerId(worker), workItems)
}

fun generateAvailability(assignmentState: AssignmentState): Map<JobId, Set<WorkerId>> {
    val availability = HashMap<JobId, Set<WorkerId>>()

    for ((key, value) in assignmentState) {
        for (workItem in value) {
            availability.computeIfAbsent(workItem.jobId) { HashSet() }
                    .add(key)
        }
    }

    return availability
}

fun generateItemsToAssign(assignmentState: AssignmentState): HashSet<WorkItem> {
    val itemsToAssign = HashSet<WorkItem>()

    for ((_, value) in assignmentState) {
        itemsToAssign.addAll(value)
    }

    return itemsToAssign
}

fun calculateReassignments(stateBefore: AssignmentState, stateAfter: AssignmentState): Int {
    var count = 0

    for ((workerId, value) in stateBefore) {

        for (workItem in value) {
            if (!stateAfter.containsWorkItemOnWorker(workerId, workItem)) {
                count++
            }
        }
    }
    return count
}

fun generateWorkItems(jobId: JobId, indexFromInclusive: Int, indexToExclusive: Int): Set<WorkItem> {
    return IntStream.range(indexFromInclusive, indexToExclusive)
            .mapToObj { index -> WorkItem("work-item-$index", jobId) }
            .collect<Set<WorkItem>, Any>(Collectors.toSet())
}

fun print(
        availability: Map<JobId, Set<WorkerId>>,
        prevAssignment: AssignmentState,
        currentAssignment: AssignmentState,
        itemsToAssign: Set<WorkItem>
) {
    logger.info(availability(availability)
            .append("Previous $prevAssignment")
            .append(itemsToAssign(itemsToAssign))
            .append("New $currentAssignment")
            .toString()
    )
}

private fun itemsToAssign(itemsToAssign: Set<WorkItem>): StringBuilder {
    val picture = StringBuilder("Items to assign:\n")
    val jobs = HashMap<JobId, Set<WorkItem>>()

    itemsToAssign.forEach { item ->
        jobs.putIfAbsent(item.jobId, HashSet()).add(item)
    }

    jobs.forEach { (jobId, workItems) ->
        picture.append("\t└ ").append(jobId).append("\n")

        workItems.forEach { workItem -> picture.append("\t\t└ ").append(workItem).append("\n") }
    }
    return picture
}

private fun availability(availability: Map<JobId, Set<WorkerId>>): StringBuilder {
    val picture = StringBuilder("Availability:\n")

    availability.forEach { (jobId, workerIds) ->
        picture.append("\t└ ").append(jobId).append("\n")

        workerIds.forEach { workerId -> picture.append("\t\t└ ").append(workerId).append("\n") }
    }
    return picture
}
