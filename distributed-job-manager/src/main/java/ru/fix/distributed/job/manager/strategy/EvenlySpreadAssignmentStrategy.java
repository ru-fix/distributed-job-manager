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

        for (AssignmentState availabilityState : availability.values()) {
            HashSet<WorkItem> itemsToAssign = new HashSet<>();
            availabilityState.values().forEach(itemsToAssign::addAll);

            int workersCount = availabilityState.size();
            int workItemsCount = itemsToAssign.size();
            int limit = workItemsCount / workersCount;
            int itemsAssignedFromPreviousCounter = 0;

            for (Map.Entry<WorkerId, HashSet<WorkItem>> workerEntry : availabilityState.entrySet()) {
                currentAssignment.putIfAbsent(workerEntry.getKey(), new HashSet<>());
            }

            for (WorkItem item : itemsToAssign) {
                WorkerId workerFromPrevious = prevAssignment.getWorkerOfWorkItem(item);

                if (prevAssignment.containsWorkItem(item)
                        && itemsAssignedFromPreviousCounter < limit
                        && currentAssignment.containsKey(workerFromPrevious)) {

                    currentAssignment.addWorkItem(workerFromPrevious, item);
                    itemsAssignedFromPreviousCounter++;
                } else {
                    WorkerId lessBusyWorker = currentAssignment
                            .getLessBusyWorkerFromAvailableWorkers(availabilityState.keySet());
                    currentAssignment.addWorkItem(lessBusyWorker, item);
                }
            }

        }

        return currentAssignment;
    }

}
