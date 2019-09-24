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

public class RendezvousHashAssignmentStrategy implements AssignmentStrategy {
    private static final Logger log = LoggerFactory.getLogger(RendezvousHashAssignmentStrategy.class);

    @Override
    public AssignmentState reassignAndBalance(
            Map<JobId, AssignmentState> availability,
            AssignmentState prevAssignment,
            AssignmentState currentAssignment
    ) {
        final Funnel<String> stringFunnel = (from, into) -> {
            try {
                into.putBytes(from.getBytes("UTF-8"));
            } catch (UnsupportedEncodingException e) {
                log.warn("Can't reassign and balance: ", e);
            }
        };


        for (Map.Entry<JobId, AssignmentState> jobEntry : availability.entrySet()) {
            final RendezvousHash<String, String> hash = new RendezvousHash<>(
                    Hashing.murmur3_128(), stringFunnel, stringFunnel, new ArrayList<>()
            );
            jobEntry.getValue().keySet().forEach(worker -> hash.add(worker.getId()));

            for (Map.Entry<WorkerId, HashSet<WorkItem>> workerEntry : jobEntry.getValue().entrySet()) {
                currentAssignment.putIfAbsent(workerEntry.getKey(), new HashSet<>());

                for (WorkItem workItem : workerEntry.getValue()) {
                    if (currentAssignment.containsWorkItem(workItem)) {
                        continue;
                    }

                    String workerId = hash.get(workItem.getJobId() + "_" + workItem.getId());
                    currentAssignment.addWorkItem(new WorkerId(workerId), workItem);
                }
            }
        }

        return currentAssignment;
    }
}
