package ru.fix.distributed.job.manager.strategy;

import com.google.common.hash.Funnel;
import com.google.common.hash.Hashing;
import ru.fix.distributed.job.manager.model.JobId;
import ru.fix.distributed.job.manager.model.WorkItem;
import ru.fix.distributed.job.manager.model.WorkerItem;
import ru.fix.distributed.job.manager.model.ZookeeperState;
import ru.fix.distributed.job.manager.util.RendezvousHash;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RendezvousHashAssignmentStrategy implements AssignmentStrategy {

    @Override
    public ZookeeperState reassignAndBalance(
            ZookeeperState availability,
            ZookeeperState prevAssignment,
            ZookeeperState newAssignment,
            Map<JobId, List<WorkItem>> itemsToAssign
    ) {

        for (Map.Entry<JobId, List<WorkItem>> jobId : itemsToAssign.entrySet()) {
            WorkerItem worker = newAssignment.getLessBusyWorker();
            for (WorkItem workItem : jobId.getValue()) {
                newAssignment.addWorkItem(worker, workItem);
            }
        }

        final Funnel<String> strFunnel = (from, into) -> into.putBytes(from.getBytes());
        final Funnel<WorkerItem> workerFunnel = (from, into) -> into.putBytes(from.getId().getBytes());

        RendezvousHash<WorkerItem, String> hash = new RendezvousHash<>(
                Hashing.murmur3_128(), workerFunnel, strFunnel, new ArrayList<>());

        for (Map.Entry<WorkerItem, List<WorkItem>> worker : newAssignment.entrySet()) {
            for (WorkItem workItem : worker.getValue()) {
                hash.add(workItem.getJobId() + "&&" + workItem.getId());
            }
        }

        ZookeeperState assignmentAfterRendezvous = new ZookeeperState();

        for (Map.Entry<WorkerItem, List<WorkItem>> worker : newAssignment.entrySet()) {
            for (WorkItem workItem : worker.getValue()) {
                String[] jobIdWorkItem = hash.get(worker.getKey()).split("&&");
                WorkItem newWorkItem = new WorkItem(jobIdWorkItem[1], jobIdWorkItem[0]);

                assignmentAfterRendezvous.addWorkItem(worker.getKey(), newWorkItem);
            }
        }

        return assignmentAfterRendezvous;
    }
}
