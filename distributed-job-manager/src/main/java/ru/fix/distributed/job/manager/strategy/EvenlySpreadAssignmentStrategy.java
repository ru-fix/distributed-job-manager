package ru.fix.distributed.job.manager.strategy;

import ru.fix.distributed.job.manager.model.AssignmentState;
import ru.fix.distributed.job.manager.model.JobId;
import ru.fix.distributed.job.manager.model.WorkItem;
import ru.fix.distributed.job.manager.model.WorkerId;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class EvenlySpreadAssignmentStrategy extends AbstractAssignmentStrategy {

    @Override
    public AssignmentState reassignAndBalance(
            Map<JobId, Set<WorkerId>> availability,
            AssignmentState prevAssignment,
            AssignmentState currentAssignment,
            Set<WorkItem> itemsToAssign
    ) {
        for (Map.Entry<JobId, Set<WorkerId>> jobEntry : availability.entrySet()) {
            Set<WorkItem> itemsToAssignForJob = getWorkItemsByJob(jobEntry.getKey(), itemsToAssign);
            jobEntry.getValue().forEach(e -> currentAssignment.putIfAbsent(e, new HashSet<>()));

            int workersCount = jobEntry.getValue().size();
            int workItemsCount = itemsToAssignForJob.size();
            int limitWorkItemsOnWorker = limitWorkItemsOnWorker(workItemsCount, workersCount);
            int itemsAssignedFromPreviousCounter = 0;

            for (WorkItem item : itemsToAssignForJob) {
                WorkerId workerFromPrevious = prevAssignment.getWorkerOfWorkItem(item);

                if (prevAssignment.containsWorkItem(item)
                        && currentAssignment.containsKey(workerFromPrevious)
                        && currentAssignment.isBalanced()
                        && currentAssignment.isBalancedByJobId(item.getJobId())
                        && itemsAssignedFromPreviousCounter < limitWorkItemsOnWorker
                ) {
                    currentAssignment.addWorkItem(workerFromPrevious, item);
                    itemsAssignedFromPreviousCounter++;

                    int limitItemsOnWorkerInPreviousState = prevAssignment
                            .get(workerFromPrevious, jobEntry.getKey()).size();
                    if (limitItemsOnWorkerInPreviousState <= itemsAssignedFromPreviousCounter) {
                        itemsAssignedFromPreviousCounter = 0;
                    }
                } else {
                    WorkerId lessBusyWorker = currentAssignment
                            .getLessBusyWorkerFromAvailableWorkers(jobEntry.getValue());
                    currentAssignment.addWorkItem(lessBusyWorker, item);
                }
                itemsToAssign.remove(item);
            }
        }
        return currentAssignment;
    }

    private int limitWorkItemsOnWorker(int itemsCount, int workersCount) {
        return itemsCount / workersCount + ((itemsCount % workersCount == 0) ? 0 : 1);
    }

}
