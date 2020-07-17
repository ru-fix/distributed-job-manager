package ru.fix.distributed.job.manager;

import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public final class WorkPool {

    private final Set<String> items;

    public WorkPool(Set<String> items) {
        this.items = new HashSet<>(items);
    }

    public static WorkPool of(Set<String> items) {
        return new WorkPool(items);
    }

    @NotNull
    public static WorkPool single() {
        return new WorkPool(Collections.singleton("singleton"));
    }

    public Set<String> getItems() {
        return items;
    }

}
