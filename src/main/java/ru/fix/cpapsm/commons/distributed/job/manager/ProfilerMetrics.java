package ru.fix.cpapsm.commons.distributed.job.manager;

/**
 * @author Kamil Asfandiyarov
 */
public final class ProfilerMetrics {
    /**
     * .jobId.start
     */
    public static String START(String jobId) {
        return jobId.replace('.', '_') + ".start";
    }

    /**
     * information about job running time, etc.
     * .jobId.stop
     */
    public static String STOP(String jobId) {
        return jobId.replace('.', '_') + ".stop";
    }

    /**
     * count of currently running jobs
     * .jobId.run
     */
    public static String RUN_INDICATOR(String jobId) {
        return jobId.replace('.', '_') + ".run";
    }
}
