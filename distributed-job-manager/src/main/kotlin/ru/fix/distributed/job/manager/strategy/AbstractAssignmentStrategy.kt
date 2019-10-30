package ru.fix.distributed.job.manager.strategy

import ru.fix.distributed.job.manager.model.JobId
import ru.fix.distributed.job.manager.model.WorkItem

abstract class AbstractAssignmentStrategy : AssignmentStrategy {

    protected fun getWorkItemsByJob(jobId: JobId, workItems: Set<WorkItem>): Set<WorkItem> {
        return workItems
                .filter { item -> item.jobId == jobId }
                .toCollection(mutableSetOf())
    }

    protected fun getWorkItemsByJobAsMap(workItems: Set<WorkItem>, jobs: Set<JobId>): Map<JobId, Set<WorkItem>> {
        return jobs.map { it to getWorkItemsByJob(it, workItems) }
                .sortedByDescending { (_, value) -> value.size }.toMap()
    }
}
