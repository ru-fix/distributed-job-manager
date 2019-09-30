package ru.fix.distributed.job.manager.strategy;

import com.google.common.hash.Funnel;
import com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.distributed.job.manager.model.AssignmentState;
import ru.fix.distributed.job.manager.model.JobId;
import ru.fix.distributed.job.manager.model.WorkItem;
import ru.fix.distributed.job.manager.model.WorkerId;
import ru.fix.distributed.job.manager.util.RendezvousHash;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RendezvousHashAssignmentStrategy extends AbstractAssignmentStrategy {
    private static final Logger log = LoggerFactory.getLogger(RendezvousHashAssignmentStrategy.class);

    @Override
    public AssignmentState reassignAndBalance(
            Map<JobId, Set<WorkerId>> availability,
            AssignmentState prevAssignment,
            AssignmentState currentAssignment,
            Set<WorkItem> itemsToAssign
    ) {
        final Funnel<String> stringFunnel = (from, into) -> {
            try {
                into.putBytes(from.getBytes("UTF-8"));
            } catch (UnsupportedEncodingException e) {
                log.warn("Can't reassign and balance: unsupported encoding", e);
            }
        };

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

                String workerId = hash.get(item.getJobId().getId() + "_" + item.getId());
                currentAssignment.addWorkItem(new WorkerId(workerId), item);
                itemsToAssign.remove(item);
            }
        }

        return currentAssignment;
    }
}
