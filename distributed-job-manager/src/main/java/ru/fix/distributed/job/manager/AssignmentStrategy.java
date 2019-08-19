package ru.fix.distributed.job.manager;

import java.util.Map;
import java.util.Set;

/**
 * @author Kamil Asfandiyarov
 */
interface AssignmentStrategy {
    /**
     * Method reassign schedulable jobs
     *
     * @param availableRepeatableJobs For each worker Map contains Set of jobs that this worker can run locally
     * @param assignedRepeatableJobs  For each worker Map contains Set of repeatable jobs assigned to it.
     * @return information about new job assignment. For each worker returned Map contains Set of
     * jobs that was assigned to worker.
     */
    Map<String, Set<String>> reassignAndBalance(Map<String, Set<String>> availableRepeatableJobs,
                                                Map<String, Set<String>> assignedRepeatableJobs);
}
