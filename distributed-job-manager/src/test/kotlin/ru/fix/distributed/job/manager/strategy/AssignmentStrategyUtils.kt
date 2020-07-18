package ru.fix.distributed.job.manager.strategy

import ru.fix.distributed.job.manager.JobId
import ru.fix.distributed.job.manager.model.*

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

class AvailabilityScope(private val availability: Availability){
    operator fun String.invoke(vararg items: String) {
        availability[JobId(this)] = items.map { WorkerId(it) }.toHashSet()
    }
}
fun availability(builder: AvailabilityScope.() -> Unit) =
        Availability().apply {
            builder(AvailabilityScope(this))
        }

class WorkItemScope(private val workItems: HashSet<WorkItem>){
    operator fun String.invoke(vararg items: String) {
        for(item in items){
            workItems.add(WorkItem(item, JobId(this)))
        }
    }
}
fun workItems(builder: WorkItemScope.() -> Unit) =
        HashSet<WorkItem>().apply {
            WorkItemScope(this).builder()
        }

fun generateAvailability(assignmentState: AssignmentState): Availability {
    val availability = Availability()

    for ((key, value) in assignmentState) {
        for (workItem in value) {
            availability.getOrPut(workItem.jobId) { hashSetOf<WorkerId>() }.add(key)
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

fun AssignmentState.getLocalWorkPoolSizeInfo(availability: Map<JobId, Set<WorkerId>>): String {
    val info = StringBuilder("Work pool size per job:\n")
    availability.forEach { (jobId: JobId?, availableWorkers: Set<WorkerId?>) ->
        info.append(jobId)
                .append(" - work pool size: ")
                .append(localPoolSize(jobId))
                .append("\n")
        availableWorkers.forEach { workerId: WorkerId? ->
            info.append("\t")
                    .append(workerId)
                    .append(": ")
                    .append(this.getWorkItems(workerId, jobId).size)
                    .append("\n")
        }

    }
    return info.toString()
}

fun AssignmentState.getGlobalWorkPoolSizeInfo(): String? {
    val info = StringBuilder("Global load per worker:\n")
    this.forEach { workerId: WorkerId?, items: HashSet<WorkItem?> ->
        info.append("\t")
                .append(workerId)
                .append(": ")
                .append(items.size)
                .append("\n")
    }
    return info.toString()
}

class Report(
        private val availability: Map<JobId, Set<WorkerId>>? = null,
        private val itemsToAssign: Set<WorkItem>? = null,
        private val prevAssignment: AssignmentState? = null,
        private val newAssignment: AssignmentState? = null
) {

    override fun toString(): String {
        return "".plus(availability?.let { availability(availability) } ?: "")
                .plus(itemsToAssign?.let { itemsToAssign(itemsToAssign) } ?: "")
                .plus(prevAssignment?.let { "Previous $prevAssignment" } ?: "")
                .plus(newAssignment?.let { "New $newAssignment" } ?: "")
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
}

