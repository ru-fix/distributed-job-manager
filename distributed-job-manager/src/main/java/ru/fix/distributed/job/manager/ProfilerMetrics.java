package ru.fix.distributed.job.manager;

/**
 * @author Kamil Asfandiyarov
 */
public final class ProfilerMetrics {
    /**
     * .jobId.start
     */
    public static String START(JobId jobId) {
        return jobId.getId().replace('.', '_') + ".start";
    }

    /**
     * information about job running time, etc.
     * .jobId.stop
     */
    public static String STOP(JobId jobId) {
        return jobId.getId().replace('.', '_') + ".stop";
    }

    /**
     * count of currently running jobs
     * .jobId.run
     */
    public static String RUN_INDICATOR(JobId jobId) {
        return jobId.getId().replace('.', '_') + ".run";
    }
}
