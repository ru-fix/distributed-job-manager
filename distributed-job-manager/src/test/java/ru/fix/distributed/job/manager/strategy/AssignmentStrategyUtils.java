package ru.fix.distributed.job.manager.strategy;

import ru.fix.distributed.job.manager.model.WorkItem;
import ru.fix.distributed.job.manager.model.WorkerId;
import ru.fix.distributed.job.manager.model.AssignmentState;

import java.util.*;

class AssignmentStrategyUtils {

    static void addWorkerWithItems(AssignmentState state, String worker, int workItemsCount, int jobsCount) {
        List<WorkItem> workItems = new ArrayList<>();

        for (int i = 0; i < workItemsCount; i++) {
            for (int j = 0; j < jobsCount; j++) {
                workItems.add(new WorkItem("work-item-" + i, "job-" + j));
            }
        }
        state.addWorkItems(new WorkerId(worker), workItems);
    }

    static AssignmentState generateCurrentState(AssignmentState available) {
        AssignmentState newAssignment = new AssignmentState();
        available.keySet().forEach(worker -> newAssignment.put(worker, new HashSet<>()));
        return newAssignment;
    }

    static Set<WorkItem> generateItemsToAssign(AssignmentState availableState) {
        Set<WorkItem> workItemsToAssign = new HashSet<>();
        availableState.values().forEach(workItemsToAssign::addAll);
        return workItemsToAssign;
    }
}
