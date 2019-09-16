package ru.fix.distributed.job.manager.strategy;

import ru.fix.distributed.job.manager.model.AssignmentState;
import ru.fix.distributed.job.manager.model.JobId;
import ru.fix.distributed.job.manager.model.WorkItem;
import ru.fix.distributed.job.manager.model.WorkerItem;

import java.util.List;
import java.util.Map;

public class EvenlySpreadAssignmentStrategy implements AssignmentStrategy {

    @Override
    public AssignmentState reassignAndBalance(
            AssignmentState availability,
            AssignmentState prevAssignment,
            AssignmentState currentAssignment,
            Map<JobId, List<WorkItem>> itemsToAssign
    ) {

        for (Map.Entry<JobId, List<WorkItem>> jobId : itemsToAssign.entrySet()) {
            for (WorkItem workItem : jobId.getValue()) {
                WorkerItem lessBusyWorker = currentAssignment.getLessBusyWorker();

                currentAssignment.addWorkItem(lessBusyWorker, workItem);
            }
        }

        return currentAssignment;
    }
}
