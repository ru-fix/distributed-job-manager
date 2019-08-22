package ru.fix.distributed.job.manager.model.distribution;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class JobState {

    private Set<WorkerItem> workers = new HashSet<>();

    public Set<WorkerItem> getWorkers() {
        return workers;
    }

    public Map<WorkerItem, Set<WorkPoolItem>> toMap() {
        return workers.stream().collect(Collectors.toMap(v -> v, WorkerItem::getWorkPools));
    }

    @Override
    public String toString() {
        return "JobState{" +
                "workers=" + workers +
                '}';
    }
}
