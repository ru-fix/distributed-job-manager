package ru.fix.distributed.job.manager;

import java.util.Collection;

/**
 * @author Ayrat Zulkarnyaev
 */
public interface WorkPoolRunningStrategy {

    int getThreadCount(Collection<String> localWorkPool);

}
