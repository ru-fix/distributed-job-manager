package ru.fix.distributed.job.manager.strategy;

import ru.fix.distributed.job.manager.model.AssignmentState;
import ru.fix.distributed.job.manager.model.JobId;
import ru.fix.distributed.job.manager.model.WorkItem;
import ru.fix.distributed.job.manager.model.WorkerId;

import java.util.HashSet;
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
     * @param availability   where (on which workers) job can launch work-items
     *  For example:
     *      distr-job-id-2 can launch on workers 0, 1 with items 0,1
     *      distr-job-id-1 can launch on workers 0, 1 with items 0,1,2
     *      distr-job-id-1 can launch on workers 0, 1 with items 0
     *
     *  Job[distr-job-id-2]=Zookeeper state
     *  └ worker-1
     * 		└ work-item-0
     * 		└ work-item-1
     * 	└ worker-0
     * 		└ work-item-0
     * 		└ work-item-1
     * , Job[distr-job-id-1]=Zookeeper state
     * 	└ worker-1
     * 		└ work-item-2
     * 		└ work-item-1
     * 		└ work-item-0
     * 	└ worker-0
     * 		└ work-item-2
     * 		└ work-item-1
     * 		└ work-item-0
     * , Job[distr-job-id-0]=Zookeeper state
     * 	└ worker-1
     * 		└ work-item-0
     * 	└ worker-0
     * 		└ work-item-0
     *
     * @param prevAssignment previous assignment state, where jobs and work-items was launch before reassignment
     * @param currentAssignment  assignment, that should be filled and returned
     * @return assignment strategy result after applying several strategies under currentAssignment
     */
    AssignmentState reassignAndBalance(
            Map<JobId, Set<WorkerId>> availability,
            AssignmentState prevAssignment,
            AssignmentState currentAssignment,
            HashSet<WorkItem> itemsToAssign
    );
}
