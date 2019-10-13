package ru.fix.distributed.job.manager.strategy

import ru.fix.distributed.job.manager.model.JobId
import ru.fix.distributed.job.manager.model.WorkItem
import java.util.stream.Collectors

abstract class AbstractAssignmentStrategy : AssignmentStrategy {

    protected fun getWorkItemsByJob(jobId: JobId, workItems: Set<WorkItem>): Set<WorkItem> {
        return workItems.stream()
                .filter { item -> item.jobId == jobId }
                .collect<Set<WorkItem>, Any>(Collectors.toSet())
    }
}
