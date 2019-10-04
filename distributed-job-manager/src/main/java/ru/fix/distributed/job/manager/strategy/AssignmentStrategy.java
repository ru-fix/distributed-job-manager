package ru.fix.distributed.job.manager.strategy;

import ru.fix.distributed.job.manager.model.AssignmentState;
import ru.fix.distributed.job.manager.model.JobId;
import ru.fix.distributed.job.manager.model.WorkItem;
import ru.fix.distributed.job.manager.model.WorkerId;

import java.util.Map;
import java.util.Set;

/**
 * Job assignment strategy which could manage work pools distribution on workers
 */
public interface AssignmentStrategy {

    /**
     * Before running all assignment strategies currentAssignment is empty
     * When we apply some assignment strategy, we fill currentAssignment with work items from itemsToAssign
     *
     * @param availability   where (on which workers) job can launch work items
     * @param prevAssignment previous assignment state, where jobs and work-items was launch before reassignment
     * @param currentAssignment  assignment, that should be filled and returned
     * @param itemsToAssign is set of work items, which should fill currentAssignment by this strategy
     * @return assignment strategy result after applying several strategies under currentAssignment
     */
    AssignmentState reassignAndBalance(
            Map<JobId, Set<WorkerId>> availability,
            AssignmentState prevAssignment,
            AssignmentState currentAssignment,
            Set<WorkItem> itemsToAssign
    );
}
