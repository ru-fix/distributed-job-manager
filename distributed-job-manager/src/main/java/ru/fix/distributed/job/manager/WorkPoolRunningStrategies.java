package ru.fix.distributed.job.manager;

import org.jetbrains.annotations.NotNull;

import java.util.Collection;

/**
 * @author Ayrat Zulkarnyaev
 */
public final class WorkPoolRunningStrategies {

    private WorkPoolRunningStrategies() {
        // Closed constructor
    }

    @NotNull
    public static WorkPoolRunningStrategy getSingleThreadStrategy() {
        return localWorkPool -> 1;
    }


    public static WorkPoolRunningStrategy getThreadPerWorkItemStrategy() {
        return Collection::size;
    }

}
