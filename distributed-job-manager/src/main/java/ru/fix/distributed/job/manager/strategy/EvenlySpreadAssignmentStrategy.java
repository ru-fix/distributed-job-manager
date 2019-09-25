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

            int workersCount = jobEntry.getValue().size();
            int workItemsCount = itemsToAssignForJob.size();
            int limit = workItemsCount / workersCount;
            int itemsAssignedFromPreviousCounter = 0;

            jobEntry.getValue().forEach(e -> currentAssignment.putIfAbsent(e, new HashSet<>()));

            for (WorkItem item : itemsToAssignForJob) {
                WorkerId workerFromPrevious = prevAssignment.getWorkerOfWorkItem(item);

                if (prevAssignment.containsWorkItem(item)
                        && itemsAssignedFromPreviousCounter < limit
                        && currentAssignment.containsKey(workerFromPrevious)) {

                    currentAssignment.addWorkItem(workerFromPrevious, item);
                    itemsAssignedFromPreviousCounter++;
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
}
