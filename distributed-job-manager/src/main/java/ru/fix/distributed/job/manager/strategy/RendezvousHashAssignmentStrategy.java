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
            ZookeeperState currentAssignment,
            Map<JobId, List<WorkItem>> itemsToAssign
    ) {
        final Funnel<String> stringFunnel = (from, into) -> into.putBytes(from.getBytes());
        final RendezvousHash<String, String> hash = new RendezvousHash<>(
                Hashing.murmur3_128(), stringFunnel, stringFunnel, new ArrayList<>());

        for (Map.Entry<WorkerItem, List<WorkItem>> worker : currentAssignment.entrySet()) {
            hash.add(worker.getKey().getId());
        }

        for (Map.Entry<JobId, List<WorkItem>> job : itemsToAssign.entrySet()) {
            for (WorkItem workItem : job.getValue()) {
                String workerId = hash.get(workItem.getJobId() + "" + workItem.getId());
                currentAssignment.addWorkItem(new WorkerItem(workerId), workItem);
            }
        }

        return currentAssignment;
    }
}
