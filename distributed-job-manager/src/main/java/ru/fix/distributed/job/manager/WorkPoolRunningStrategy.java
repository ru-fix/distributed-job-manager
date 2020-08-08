package ru.fix.distributed.job.manager;

import java.util.Set;

/**
 * @author Ayrat Zulkarnyaev
 */
@FunctionalInterface
public interface WorkPoolRunningStrategy {
    /**
     * @param workShare WorkItems assigned to current [{@link DistributedJobManager}] instance for given Job type.
     * @return how many threads should be used to process workShare
     */
    int getThreadCount(Set<String> workShare);

}
