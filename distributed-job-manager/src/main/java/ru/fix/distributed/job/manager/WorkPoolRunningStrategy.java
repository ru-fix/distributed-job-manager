package ru.fix.distributed.job.manager;

import java.util.Set;

/**
 * @author Ayrat Zulkarnyaev
 */
@FunctionalInterface
public interface WorkPoolRunningStrategy {
    int getThreadCount(Set<String> localWorkPool);

}
