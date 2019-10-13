package ru.fix.distributed.job.manager.strategy

import org.junit.platform.commons.logging.LoggerFactory
import ru.fix.distributed.job.manager.model.AssignmentState
import ru.fix.distributed.job.manager.model.JobId
import ru.fix.distributed.job.manager.model.WorkItem
import ru.fix.distributed.job.manager.model.WorkerId

fun addWorkerWithItems(state: AssignmentState, worker: String, workItemsCount: Int, jobsCount: Int) {
    val workItems = HashSet<WorkItem>()

    for (i in 0 until workItemsCount) {
        for (j in 0 until jobsCount) {
            workItems.add(WorkItem("work-item-$i", JobId("job-$j")))
        }
    }
    state.addWorkItems(WorkerId(worker), workItems)
}

fun generateAvailability(assignmentState: AssignmentState): MutableMap<JobId, MutableSet<WorkerId>> {
    val availability = mutableMapOf<JobId, MutableSet<WorkerId>>()

    for ((key, value) in assignmentState) {
        for (workItem in value) {
            availability.getOrPut(workItem.jobId) { mutableSetOf() }.add(key)
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
    return (indexFromInclusive..indexToExclusive)
            .map { WorkItem("work-item-$it", jobId) }
            .toCollection(mutableSetOf())
}

class Print(
        private val availability: Map<JobId, Set<WorkerId>>?,
        private val prevAssignment: AssignmentState?,
        private val currentAssignment: AssignmentState?,
        private val itemsToAssign: Set<WorkItem>?
) {

    override fun toString(): String {
        return """
                $availability
                $itemsToAssign
                $prevAssignment
                $currentAssignment
           """.trimIndent()
    }

    private fun itemsToAssign(itemsToAssign: Set<WorkItem>): StringBuilder {
        val picture = StringBuilder("Items to assign:\n")
        val jobs = HashMap<JobId, MutableSet<WorkItem>>()

        itemsToAssign.forEach { item ->
            jobs.getOrPut(item.jobId) { mutableSetOf() }.add(item)
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

    data class Builder(
            private var availability: Map<JobId, Set<WorkerId>>? = null,
            private var prevAssignment: AssignmentState? = null,
            private var currentAssignment: AssignmentState? = null,
            private var itemsToAssign: Set<WorkItem>? = null
    ) {
        fun availability(availability: Map<JobId, Set<WorkerId>>) = apply { this.availability = availability }
        fun previousAssignment(assignment: AssignmentState) = apply { this.prevAssignment = assignment }
        fun currentAssignment(assignment: AssignmentState) = apply { this.currentAssignment = assignment }
        fun itemsToAssign(itemsToAssign: Set<WorkItem>) = apply { this.itemsToAssign = itemsToAssign }
        fun build() = Print(availability, prevAssignment, currentAssignment, itemsToAssign)
    }
}

