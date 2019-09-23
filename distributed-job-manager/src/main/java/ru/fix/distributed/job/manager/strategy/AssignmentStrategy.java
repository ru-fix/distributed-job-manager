package ru.fix.distributed.job.manager.strategy;

import ru.fix.distributed.job.manager.model.AssignmentState;
import ru.fix.distributed.job.manager.model.JobId;

import java.util.Map;

/**
 * Job assignment strategy which could manage work pools distribution on workers
 */
public interface AssignmentStrategy {
    /**
     * Before running all assignment strategies currentAssignment is empty
     * When we apply some assignment strategy, we fill currentAssignment with work items from itemsToAssign
     *
     * @param availability   where (on which workers) job can launch work-items
     * @param prevAssignment previous assignment state, where jobs and work-items was launch before reassignment
     * @param currentAssignment  assignment, that should be filled and returned
     * @return assignment strategy result after applying several strategies under currentAssignment
     */
    AssignmentState reassignAndBalance(
            Map<JobId, AssignmentState> availability,
            AssignmentState prevAssignment,
            AssignmentState currentAssignment
    );
}
