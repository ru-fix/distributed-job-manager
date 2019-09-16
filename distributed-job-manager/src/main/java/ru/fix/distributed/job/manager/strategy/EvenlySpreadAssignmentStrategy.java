package ru.fix.distributed.job.manager.strategy;

import ru.fix.distributed.job.manager.model.AssignmentState;
import ru.fix.distributed.job.manager.model.JobId;
import ru.fix.distributed.job.manager.model.WorkItem;
import ru.fix.distributed.job.manager.model.WorkerItem;

import java.util.ArrayList;
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
        int workersCount = currentAssignment.size();
        if (workersCount == 0) {
            return currentAssignment;
        }

        int itemsToAssignSize = itemsToAssign.values().stream()
                .mapToInt(List::size)
                .sum();
        int canBeTakenFromPreviousPerWorker = itemsToAssignSize / workersCount;

        for (Map.Entry<WorkerItem, List<WorkItem>> worker : prevAssignment.entrySet()) {
            int itemsAddedFromPrevious = 0;

            for (WorkItem workItem : worker.getValue()) {
                if (!currentAssignment.containsKey(worker.getKey())) {
                   continue;
                }
                if (itemsAddedFromPrevious >= canBeTakenFromPreviousPerWorker) {
                    break;
                }
                currentAssignment.addWorkItem(worker.getKey(), workItem);
                removeWorkItem(itemsToAssign, workItem);
                itemsAddedFromPrevious++;
            }
        }

        for (Map.Entry<JobId, List<WorkItem>> jobId : itemsToAssign.entrySet()) {
            for (WorkItem workItem : jobId.getValue()) {
                WorkerItem lessBusyWorker = currentAssignment.getLessBusyWorker();

                currentAssignment.addWorkItem(lessBusyWorker, workItem);
            }
        }

        return currentAssignment;
    }

    private void removeWorkItem(Map<JobId, List<WorkItem>> itemsToAssign, WorkItem workItemToRemove) {
        for (Map.Entry<JobId, List<WorkItem>> jobId : itemsToAssign.entrySet()) {
            for (WorkItem workItem : jobId.getValue()) {
                if (workItem.equals(workItemToRemove)) {
                    List<WorkItem> withoutRemoved = new ArrayList<>(jobId.getValue());
                    withoutRemoved.remove(workItem);
                    itemsToAssign.replace(jobId.getKey(), withoutRemoved);
                }
            }
        }
    }
}
