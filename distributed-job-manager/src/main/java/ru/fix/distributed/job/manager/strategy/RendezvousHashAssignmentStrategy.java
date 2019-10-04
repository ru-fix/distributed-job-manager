package ru.fix.distributed.job.manager.strategy;

import com.google.common.hash.Funnel;
import com.google.common.hash.Hashing;
import ru.fix.distributed.job.manager.model.AssignmentState;
import ru.fix.distributed.job.manager.model.JobId;
import ru.fix.distributed.job.manager.model.WorkItem;
import ru.fix.distributed.job.manager.model.WorkerId;
import ru.fix.distributed.job.manager.util.RendezvousHash;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RendezvousHashAssignmentStrategy extends AbstractAssignmentStrategy {

    @SuppressWarnings("UnstableApiUsage")
    @Override
    public AssignmentState reassignAndBalance(
            Map<JobId, Set<WorkerId>> availability,
            AssignmentState prevAssignment,
            AssignmentState currentAssignment,
            Set<WorkItem> itemsToAssign
    ) {
        final Funnel<String> stringFunnel = (from, into) -> into.putBytes(from.getBytes(StandardCharsets.UTF_8));

        for (Map.Entry<JobId, Set<WorkerId>> jobEntry : availability.entrySet()) {
            final RendezvousHash<String, String> hash = new RendezvousHash<>(
                    Hashing.murmur3_128(), stringFunnel, stringFunnel, new ArrayList<>()
            );
            jobEntry.getValue().forEach(worker -> {
                hash.add(worker.getId());
                currentAssignment.putIfAbsent(worker, new HashSet<>());
            });

            Set<WorkItem> availableItems = getWorkItemsByJob(jobEntry.getKey(), itemsToAssign);
            for (WorkItem item : availableItems) {
                if (currentAssignment.containsWorkItem(item)) {
                    continue;
                }

                String workerId = hash.get(item.getJobId().getId() + ":" + item.getId());
                currentAssignment.addWorkItem(new WorkerId(workerId), item);
                itemsToAssign.remove(item);
            }
        }

        return currentAssignment;
    }
}
