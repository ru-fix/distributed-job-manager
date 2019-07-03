package ru.fix.cpapsm.commons.distributed.job.manager.strategy;

import ru.fix.cpapsm.commons.distributed.job.manager.model.distribution.JobState;

/**
 * Job assignment strategy which could manage work pools distribution on workers
 */
public interface AssignmentStrategy {

    /**
     * Reassigns job
     *
     * @param jobAvailability   contains information about availability work pools availability on workers
     * @param currentAssignment contains information about work pools assignment on workers
     * @return new assignment of work pools on workers
     */
    JobState reassignAndBalance(JobState jobAvailability, JobState currentAssignment);
}
