package ru.fix.distributed.job.manager.strategy;

import ru.fix.distributed.job.manager.model.AssignmentState;
import ru.fix.distributed.job.manager.model.WorkItem;
import ru.fix.distributed.job.manager.model.WorkerId;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EvenlySpreadAssignmentStrategy implements AssignmentStrategy {

    @Override
    public AssignmentState reassignAndBalance(
            AssignmentState availability,
            AssignmentState prevAssignment,
            AssignmentState currentAssignment,
            Set<WorkItem> itemsToAssign
    ) {
        int workersCount = currentAssignment.size();
        if (workersCount == 0) {
            return currentAssignment;
        }

        int itemsToAssignSize = itemsToAssign.size(); // TODO !!!!
        int canBeTakenFromPreviousPerWorker = itemsToAssignSize / workersCount;

        for (Map.Entry<WorkerId, HashSet<WorkItem>> worker : prevAssignment.entrySet()) {
            int itemsAddedFromPrevious = 0;

            for (WorkItem workItem : worker.getValue()) {
                if (!currentAssignment.containsKey(worker.getKey())) {
                    continue;
                }
                if (itemsAddedFromPrevious >= canBeTakenFromPreviousPerWorker) {
                    break;
                }
                currentAssignment.addWorkItem(worker.getKey(), workItem);
                itemsToAssign.remove(workItem);
                itemsAddedFromPrevious++;
            }
        }

        for (WorkItem workItem : itemsToAssign) {
            WorkerId lessBusyWorker = currentAssignment.getLessBusyWorker();
            currentAssignment.addWorkItem(lessBusyWorker, workItem);
        }

        return currentAssignment;
    }

}
