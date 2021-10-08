package ru.fix.distributed.job.manager.strategy

import ru.fix.distributed.job.manager.model.AssignmentState
import ru.fix.distributed.job.manager.model.Availability
import ru.fix.distributed.job.manager.model.WorkItem

/**
 * Job assignment strategy which could manage work pools distribution on workers
 */
interface AssignmentStrategy {

    /**
     * Before running all assignment strategies currentAssignment is empty
     * When we apply an assignment strategy, we fill currentAssignment with work items from itemsToAssign
     * @param availability   where (on which workers) job can launch work items
     * @param prevAssignment previous assignment state, where jobs and work-items were launched before reassignment
     * @param currentAssignment  assignment, that should be modified and filled with work items.
     *                           Will hold assignment strategy result.
     *                           Same assignment state can be passed through several strategies as currentAssignment.
     *                           This way result of several assignment strategies will be accumulated in same
     *                           assignment state.
     * @param itemsToAssign set of work items, which should fill currentAssignment by this strategy
     */
    fun reassignAndBalance(
        availability: Availability,
        prevAssignment: AssignmentState,
        currentAssignment: AssignmentState,
        itemsToAssign: MutableSet<WorkItem>
    )
}
