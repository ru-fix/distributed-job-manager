package ru.fix.distributed.job.manager.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.distributed.job.manager.model.JobId;
import ru.fix.distributed.job.manager.model.WorkItem;
import ru.fix.distributed.job.manager.model.ZookeeperState;

import java.util.List;
import java.util.Map;

public class RendezvousHashAssignmentStrategy implements AssignmentStrategy {
    private static final Logger log = LoggerFactory.getLogger(RendezvousHashAssignmentStrategy.class);

    @Override
    public void reassignAndBalance(
            ZookeeperState availability,
            ZookeeperState prevAssignment,
            ZookeeperState newAssignment,
            Map<JobId, List<WorkItem>> itemsToAssign
    ) {
        log.info(availability.toString());
        log.info(prevAssignment.toString());
        log.info(newAssignment.toString());

        log.info(itemsToAssign.toString());

        newAssignment = availability;
    }
}
