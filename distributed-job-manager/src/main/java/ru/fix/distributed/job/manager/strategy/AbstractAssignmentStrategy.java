package ru.fix.distributed.job.manager.strategy;

import ru.fix.distributed.job.manager.model.JobId;
import ru.fix.distributed.job.manager.model.WorkItem;

import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractAssignmentStrategy implements AssignmentStrategy {

    protected Set<WorkItem> getWorkItemsByJob(JobId jobId, Set<WorkItem> workItems) {
        return workItems.stream()
                .filter(item -> item.getJobId().equals(jobId))
                .collect(Collectors.toSet());
    }
}
