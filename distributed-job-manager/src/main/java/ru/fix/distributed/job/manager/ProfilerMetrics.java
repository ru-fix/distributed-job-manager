package ru.fix.distributed.job.manager;

/**
 * @author Kamil Asfandiyarov
 */
public final class ProfilerMetrics {
    public static String DJM_PREFIX = "djm";

    public static String JOB(JobId jobId) {
        return "job." + jobId.getId().replace('.', '_').replace('-', '_');
    }

    public static String DJM_INIT = "init";
    public static String DJM_CLOSE = "close";

}
