package ru.fix.distributed.job.manager.strategy;

import com.google.common.hash.Funnel;
import com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.distributed.job.manager.model.JobId;
import ru.fix.distributed.job.manager.model.WorkItem;
import ru.fix.distributed.job.manager.model.WorkerId;
import ru.fix.distributed.job.manager.model.AssignmentState;
import ru.fix.distributed.job.manager.util.RendezvousHash;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RendezvousHashAssignmentStrategy implements AssignmentStrategy {
    private static final Logger log = LoggerFactory.getLogger(RendezvousHashAssignmentStrategy.class);

    @Override
    public AssignmentState reassignAndBalance(
            AssignmentState availability,
            AssignmentState prevAssignment,
            AssignmentState currentAssignment,
            Set<WorkItem> itemsToAssign
    ) {
        final Funnel<String> stringFunnel = (from, into) -> {
            try {
                into.putBytes(from.getBytes("UTF-8"));
            } catch (UnsupportedEncodingException e) {
                log.warn("Can't reassign and balance: ", e);
            }
        };
        final RendezvousHash<String, String> hash = new RendezvousHash<>(
                Hashing.murmur3_128(), stringFunnel, stringFunnel, new ArrayList<>()
        );

        for (WorkerId worker : availability.keySet()) {
            hash.add(worker.getId());
        }

        for (WorkItem workItem : itemsToAssign) {
            String workerId = hash.get(workItem.getJobId() + "" + workItem.getId());
            currentAssignment.addWorkItem(new WorkerId(workerId), workItem);
        }

        return currentAssignment;
    }
}
