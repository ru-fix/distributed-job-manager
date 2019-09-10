package ru.fix.distributed.job.manager.strategy;

import ru.fix.distributed.job.manager.model.JobId;
import ru.fix.distributed.job.manager.model.WorkItem;
import ru.fix.distributed.job.manager.model.WorkerItem;
import ru.fix.distributed.job.manager.model.ZookeeperState;

import java.util.List;
import java.util.Map;

public class EvenlySpreadAssignmentStrategy implements AssignmentStrategy {

    @Override
    public ZookeeperState reassignAndBalance(
            ZookeeperState availability,
            ZookeeperState prevAssignment,
            ZookeeperState newAssignment,
            Map<JobId, List<WorkItem>> itemsToAssign
    ) {

        for (Map.Entry<JobId, List<WorkItem>> jobId : itemsToAssign.entrySet()) {
            for (WorkItem workItem : jobId.getValue()) {
                WorkerItem lessBusyWorker = newAssignment.getLessBusyWorker();

                newAssignment.addWorkItem(lessBusyWorker, workItem);
            }
        }

        return newAssignment;
    }
}