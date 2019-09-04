package ru.fix.distributed.job.manager.model;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class WorkerItem {

    private String id;
    private Set<WorkItem> workPools = new HashSet<>();

    public WorkerItem(String id) {
        Objects.requireNonNull(id);
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public Set<WorkItem> getWorkPools() {
        return workPools;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WorkerItem that = (WorkerItem) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return "WorkerItem{" +
                "id='" + id + '\'' +
                ", workPools=" + workPools +
                '}';
    }
}
