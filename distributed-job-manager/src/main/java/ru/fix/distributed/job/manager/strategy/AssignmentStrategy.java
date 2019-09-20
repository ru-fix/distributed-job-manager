package ru.fix.distributed.job.manager.strategy;

import ru.fix.distributed.job.manager.model.JobId;
import ru.fix.distributed.job.manager.model.WorkItem;
import ru.fix.distributed.job.manager.model.AssignmentState;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Job assignment strategy which could manage work pools distribution on workers
 */
public interface AssignmentStrategy {
    /**
     * At the beginning currentAssignment contains workers all workers from availability without work items
     * After applying several assignment strategies we fill currentAssignment with work items from itemsToAssign
     *
     * @param availability   where (on which workers) job can launch work-items
     * @param prevAssignment previous assignment state, where jobs and work-items was launch before reassignment
     * @param currentAssignment  all workers from availability with empty lists of work items
     * @param itemsToAssign  items that should be assigned by this strategy
     * @return assignment strategy result after applying several strategies under currentAssignment
     */
    AssignmentState reassignAndBalance(
            AssignmentState availability,
            AssignmentState prevAssignment,
            AssignmentState currentAssignment,
            Set<WorkItem> itemsToAssign
    );
}
