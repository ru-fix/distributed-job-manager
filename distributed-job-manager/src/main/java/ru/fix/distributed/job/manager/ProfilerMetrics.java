package ru.fix.distributed.job.manager;
import ru.fix.distributed.job.manager.model.JobDisableConfig;

/**
 * @author Kamil Asfandiyarov
 */
public final class ProfilerMetrics {

    /**
     * @see JobDisableConfig#getDisableAllJobs()
     */
    public static String DISABLE_ALL_JOBS_INDICATOR = "disable_all_jobs";

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
