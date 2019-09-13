package ru.fix.distributed.job.manager.strategy;

import ru.fix.distributed.job.manager.model.JobId;
import ru.fix.distributed.job.manager.model.WorkItem;
import ru.fix.distributed.job.manager.model.WorkerItem;
import ru.fix.distributed.job.manager.model.ZookeeperState;

import java.util.*;

class AssignmentStrategyUtils {
    static void addWorkerWithItems(ZookeeperState state, String worker, int workItemsCount, int jobsCount) {
        List<WorkItem> workItems = new ArrayList<>();

        for (int i = 0; i < workItemsCount; i++) {
            for (int j = 0; j < jobsCount; j++) {
                workItems.add(new WorkItem("work-item-" + i, "job-" + j));
            }
        }
        state.addWorkItems(new WorkerItem(worker), workItems);
    }

    static ZookeeperState generateCurrentState(ZookeeperState available, ZookeeperState current) {
        ZookeeperState newAssignment = new ZookeeperState();

        for (Map.Entry<WorkerItem, List<WorkItem>> worker : current.entrySet()) {
            if (available.containsKey(worker.getKey())) {
                newAssignment.addWorkItems(worker.getKey(), Collections.emptyList());
            }
        }
        for (Map.Entry<WorkerItem, List<WorkItem>> worker : available.entrySet()) {
            if (!current.containsKey(worker.getKey())) {
                newAssignment.addWorkItems(worker.getKey(), Collections.emptyList());
            }
        }
        return newAssignment;
    }

    static Map<JobId, List<WorkItem>> generateItemsToAssign(
            ZookeeperState availableState,
            ZookeeperState currentState
    ) {
        Map<JobId, List<WorkItem>> workItemsToAssign = new HashMap<>();

        for (Map.Entry<WorkerItem, List<WorkItem>> worker : availableState.entrySet()) {
            for (WorkItem workItem : worker.getValue()) {
                String jobId = workItem.getJobId();

                if (currentState.containsWorkItem(workItem)) {
                    continue;
                }

                if (workItemsToAssign.containsKey(new JobId(jobId))) {
                    List<WorkItem> workItemsOld = new ArrayList<>(workItemsToAssign.get(new JobId(jobId)));
                    workItemsOld.add(workItem);
                    workItemsToAssign.put(new JobId(jobId), workItemsOld);
                } else {
                    workItemsToAssign.put(new JobId(jobId), Collections.singletonList(workItem));
                }
            }
        }
        return workItemsToAssign;
    }
}
