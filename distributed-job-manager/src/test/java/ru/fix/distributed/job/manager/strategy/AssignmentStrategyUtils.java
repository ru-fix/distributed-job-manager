package ru.fix.distributed.job.manager.strategy;

import ru.fix.distributed.job.manager.model.JobId;
import ru.fix.distributed.job.manager.model.WorkItem;
import ru.fix.distributed.job.manager.model.WorkerId;
import ru.fix.distributed.job.manager.model.AssignmentState;

import java.util.*;
import java.util.stream.Collectors;

class AssignmentStrategyUtils {

    static void addWorkerWithItems(AssignmentState state, String worker, int workItemsCount, int jobsCount) {
        HashSet<WorkItem> workItems = new HashSet<>();

        for (int i = 0; i < workItemsCount; i++) {
            for (int j = 0; j < jobsCount; j++) {
                workItems.add(new WorkItem("work-item-" + i, "job-" + j));
            }
        }
        state.addWorkItems(new WorkerId(worker), workItems);
    }

    static Map<JobId, AssignmentState> generateAvailability(AssignmentState assignmentState) {
        Map<JobId, AssignmentState> availability = new HashMap<>();

        for (Map.Entry<WorkerId, HashSet<WorkItem>> workerEntry : assignmentState.entrySet()) {
            for (WorkItem workItem : workerEntry.getValue()) {
                availability.computeIfAbsent(
                        new JobId(workItem.getJobId()), state -> new AssignmentState()
                ).addWorkItems(workerEntry.getKey(), workerEntry.getValue()
                        .stream().filter(e -> e.getJobId().equals(workItem.getJobId())).collect(Collectors.toSet()));
            }
        }

        return availability;
    }
}
