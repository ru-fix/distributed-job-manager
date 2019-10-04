package ru.fix.distributed.job.manager.strategy;

import ru.fix.distributed.job.manager.model.JobId;
import ru.fix.distributed.job.manager.model.WorkItem;
import ru.fix.distributed.job.manager.model.WorkerId;
import ru.fix.distributed.job.manager.model.AssignmentState;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class AssignmentStrategyUtils {

    static void addWorkerWithItems(AssignmentState state, String worker, int workItemsCount, int jobsCount) {
        HashSet<WorkItem> workItems = new HashSet<>();

        for (int i = 0; i < workItemsCount; i++) {
            for (int j = 0; j < jobsCount; j++) {
                workItems.add(new WorkItem("work-item-" + i,  new JobId("job-" + j)));
            }
        }
        state.addWorkItems(new WorkerId(worker), workItems);
    }

    static Map<JobId, Set<WorkerId>> generateAvailability(AssignmentState assignmentState) {
        Map<JobId, Set<WorkerId>> availability = new HashMap<>();

        for (Map.Entry<WorkerId, HashSet<WorkItem>> workerEntry : assignmentState.entrySet()) {
            for (WorkItem workItem : workerEntry.getValue()) {
                availability.computeIfAbsent(workItem.getJobId(), state -> new HashSet<>())
                        .add(workerEntry.getKey());
            }
        }

        return availability;
    }

    static HashSet<WorkItem> generateItemsToAssign(AssignmentState assignmentState) {
        HashSet<WorkItem> itemsToAssign = new HashSet<>();

        for (Map.Entry<WorkerId, HashSet<WorkItem>> workerEntry : assignmentState.entrySet()) {
            itemsToAssign.addAll(workerEntry.getValue());
        }

        return itemsToAssign;
    }

    static int calculateReassignments(AssignmentState stateBefore, AssignmentState stateAfter) {
        int count = 0;

        for (Map.Entry<WorkerId, HashSet<WorkItem>> stateEntry : stateBefore.entrySet()) {
            WorkerId workerId = stateEntry.getKey();

            for (WorkItem workItem : stateEntry.getValue()) {
                if (!stateAfter.containsWorkItemOnWorker(workerId, workItem)) {
                    count++;
                }
            }
        }
        return count;
    }

    static Set<WorkItem> generateWorkItems(JobId jobId, int indexFromInclusive, int indexToExclusive) {
        return IntStream.range(indexFromInclusive, indexToExclusive)
                .mapToObj(index -> new WorkItem("work-item-" + index, jobId) )
                .collect(Collectors.toSet());
    }
}
