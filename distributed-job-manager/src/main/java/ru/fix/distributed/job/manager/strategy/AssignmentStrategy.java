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
     * At the beginning currentAssignment contains workers = ((available workers - workers from prev assignment) +
     * + (workers from prev assignment - inaccessible workers))
     * After applying several assignment strategies we fill currentAssignment with work items from itemsToAssign
     *
     * @param availability   where (on which workers) job can launch work-items
     * @param prevAssignment previous assignment state, where jobs and work-items was launch before reassignment
     * @param currentAssignment  ((available workers - workers from prev assignment)
     *                          + (workers from prev assignment - inaccessible workers))
     * @param itemsToAssign  items that should be assigned by this strategy
     * @return assignment strategy result after applying several strategies under currentAssignment
     */
    ZookeeperState reassignAndBalance(
            ZookeeperState availability,
            ZookeeperState prevAssignment,
            ZookeeperState currentAssignment,
            Map<JobId, List<WorkItem>> itemsToAssign
    );
}
