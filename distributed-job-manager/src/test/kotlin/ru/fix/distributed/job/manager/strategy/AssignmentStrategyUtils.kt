package ru.fix.distributed.job.manager.strategy

import ru.fix.distributed.job.manager.model.AssignmentState
import ru.fix.distributed.job.manager.model.JobId
import ru.fix.distributed.job.manager.model.WorkItem
import ru.fix.distributed.job.manager.model.WorkerId

class WorkerScope(private val state: AssignmentState) {
    operator fun String.invoke(builder: JobScope.() -> Unit) {
        builder(JobScope(state, this))
    }
}

class JobScope(
        private val state: AssignmentState,
        private val worker: String
) {
    operator fun String.invoke(vararg items: String) {
        for (item in items) {
            state.addWorkItem(WorkerId(worker), WorkItem(item, JobId(this)))
        }
    }
}

fun assignmentState(builder: WorkerScope.() -> Unit) =
        AssignmentState().apply {
            builder(WorkerScope(this))
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
    var reassignments = 0

    for ((workerId, workItems) in stateAfter) {
        for (workItem in workItems) {
            if (!stateBefore.containsWorkItemOnWorker(workerId, workItem)) {
                reassignments++
            }
        }
    }
    return reassignments
}

class Print(
        private val availability: Map<JobId, Set<WorkerId>>?,
        private val itemsToAssign: Set<WorkItem>?,
        private val prevAssignment: AssignmentState?,
        private val newAssignment: AssignmentState?,
        private val evenlySpreadNewAssignment: AssignmentState?,
        private val rendezvousNewAssignment: AssignmentState?
) {

    override fun toString(): String {
        return "".plus(availability?.let { availability(availability) } ?: "")
                .plus(itemsToAssign?.let { itemsToAssign(itemsToAssign) } ?: "")
                .plus(prevAssignment?.let { "Previous $prevAssignment" } ?: "")
                .plus(newAssignment?.let { "New $newAssignment" } ?: "")
                .plus(evenlySpreadNewAssignment?.let { "New evenly spread $evenlySpreadNewAssignment" } ?: "")
                .plus(rendezvousNewAssignment?.let { "New rendezvous $rendezvousNewAssignment" } ?: "")
    }

    private fun itemsToAssign(itemsToAssign: Set<WorkItem>): String {
        val jobs = HashMap<JobId, MutableSet<WorkItem>>()
        itemsToAssign.forEach { item ->
            jobs.getOrPut(item.jobId) { mutableSetOf() }.add(item)
        }

        val picture = StringBuilder("Items to assign:\n")
        jobs.forEach { (jobId, workItems) ->
            picture.append("\t└ ${jobId.id}\n ")
            workItems.forEach { workItem -> picture.append("\t\t└ ${workItem.id}\n") }
        }
        return picture.toString()
    }

    private fun availability(availability: Map<JobId, Set<WorkerId>>): String {
        val picture = StringBuilder("Availability:\n")

        availability.forEach { (jobId, workerIds) ->
            picture.append("\t└ ${jobId.id}\n")
            workerIds.forEach { workerId -> picture.append("\t\t└ ${workerId.id}\n") }
        }
        return picture.toString()
    }

    data class Builder(
            private var availability: Map<JobId, Set<WorkerId>>? = null,
            private var itemsToAssign: Set<WorkItem>? = null,
            private var prevAssignment: AssignmentState? = null,
            private var newAssignment: AssignmentState? = null,
            private var evenlySpreadNewAssignment: AssignmentState? = null,
            private var rendezvousNewAssignment: AssignmentState? = null
    ) {
        fun availability(availability: Map<JobId, Set<WorkerId>>) = apply { this.availability = availability }
        fun itemsToAssign(itemsToAssign: Set<WorkItem>) = apply { this.itemsToAssign = itemsToAssign }
        fun previousAssignment(assignment: AssignmentState) = apply { this.prevAssignment = assignment }
        fun newAssignment(assignment: AssignmentState) = apply { this.newAssignment = assignment }
        fun evenlySpreadNewAssignment(assignment: AssignmentState) = apply { this.evenlySpreadNewAssignment = assignment }
        fun rendezvousNewAssignment(assignment: AssignmentState) = apply { this.rendezvousNewAssignment = assignment }
        fun build() = Print(
                availability,
                itemsToAssign,
                prevAssignment,
                newAssignment,
                evenlySpreadNewAssignment,
                rendezvousNewAssignment
        )
    }
}

