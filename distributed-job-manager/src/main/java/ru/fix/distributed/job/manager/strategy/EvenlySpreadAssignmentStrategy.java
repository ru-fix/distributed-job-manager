package ru.fix.distributed.job.manager.strategy;

import ru.fix.distributed.job.manager.model.AssignmentState;
import ru.fix.distributed.job.manager.model.JobId;
import ru.fix.distributed.job.manager.model.WorkItem;
import ru.fix.distributed.job.manager.model.WorkerId;

import java.util.HashSet;
import java.util.Map;

public class EvenlySpreadAssignmentStrategy implements AssignmentStrategy {

    @Override
    public AssignmentState reassignAndBalance(
            Map<JobId, AssignmentState> availability,
            AssignmentState prevAssignment,
            AssignmentState currentAssignment
    ) {
        for (AssignmentState ava : availability.values()) {
            for (Map.Entry<WorkerId, HashSet<WorkItem>> worker : ava.entrySet()) {
                for (WorkItem item : worker.getValue()) {
                    WorkerId lessBusyWorker = ava.getLessBusyWorker();
                    currentAssignment.addWorkItem(lessBusyWorker, item);
                }
            }
        }

        return currentAssignment;
    }

}
