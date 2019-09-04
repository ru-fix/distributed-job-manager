package ru.fix.distributed.job.manager.strategy;

import ru.fix.distributed.job.manager.model.JobId;
import ru.fix.distributed.job.manager.model.WorkItem;
import ru.fix.distributed.job.manager.model.ZookeeperState;

import java.util.List;
import java.util.Map;

/**
 * Job assignment strategy which could manage work pools distribution on workers
 */
public interface AssignmentStrategy {
    /**
     * At the beginning newAssignment is empty.
     * After applying several assignment strategies we will end up with populated newAssignment
     *
     * @param availability   where (on which workers) job can launch work-items
     * @param prevAssignment previous assignment state, where jobs and work-items was launch before reassignment
     * @param newAssignment  assignment strategy result
     * @param itemsToAssign  items that should be assigned by this strategy
     */
    void reassignAndBalance(
            ZookeeperState availability,
            ZookeeperState prevAssignment,
            ZookeeperState newAssignment,
            Map<JobId, List<WorkItem>> itemsToAssign);

}
