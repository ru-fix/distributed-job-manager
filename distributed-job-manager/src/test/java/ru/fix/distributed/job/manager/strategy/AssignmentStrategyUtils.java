package ru.fix.distributed.job.manager.strategy;

import ru.fix.distributed.job.manager.model.JobId;
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

        for (Map.Entry<WorkerId, List<WorkItem>> availableWorker : available.entrySet()) {
            newAssignment.put(availableWorker.getKey(), new ArrayList<>());
        }
        return newAssignment;
    }

    static Map<JobId, List<WorkItem>> generateItemsToAssign(AssignmentState availableState) {
        Map<JobId, List<WorkItem>> workItemsToAssign = new HashMap<>();

        for (Map.Entry<WorkerId, List<WorkItem>> worker : availableState.entrySet()) {
            for (WorkItem workItem : worker.getValue()) {
                String jobId = workItem.getJobId();

                if (workItemsToAssign.containsKey(new JobId(jobId))) {
                    List<WorkItem> workItemsOld = new ArrayList<>(workItemsToAssign.get(new JobId(jobId)));
                    workItemsOld.add(workItem);
                    workItemsToAssign.put(new JobId(jobId), workItemsOld);
                } else {
                    workItemsToAssign.put(new JobId(jobId), Arrays.asList(workItem));
                }
            }
        }
        return workItemsToAssign;
    }
}
