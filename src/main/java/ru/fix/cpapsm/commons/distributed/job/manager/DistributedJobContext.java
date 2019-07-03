package ru.fix.cpapsm.commons.distributed.job.manager;

import java.util.Set;

/**
 * @author Ayrat Zulkarnyaev
 */
public interface DistributedJobContext {

    /**
     * <p>
     * Return workflow for distributed job manager.
     * </p>
     * <p>
     * For example: if job work pool contains work items ["1", "2", "3", ... , "n"] and two DJM workers is registered,
     * then for first job will receive work share ["1", "2", ... , "n/2"] and second job will receive work share
     * ["n/2+1", ... ,* "n"].
     * </p>
     */
    Set<String> getWorkShare();

    /**
     * Indicates that current job should shutdown as soon as possible
     */
    boolean isNeedToShutdown();

    /**
     * Add shutdown listener on context close
     * Shutdown listener could be called immediately if DJM already asked job to close.
     * Shutdown listener could be called several times in edge cases.
     *
     * @param listener shutdown listener
     */
    void addShutdownListener(ShutdownListener listener);

    /**
     * Remove shutdown listener on context close
     *
     * @param listener shutdown listener
     */
    void removeShutdownListener(ShutdownListener listener);

}
