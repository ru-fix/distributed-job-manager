package ru.fix.distributed.job.manager.model.distribution;

import java.util.Objects;

public class WorkItem {

    private String id;

    public WorkItem(String id) {
        Objects.requireNonNull(id);
        this.id = id;
    }

    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WorkItem that = (WorkItem) o;
        return id.equals(that.id);

    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return "WorkPoolItem[" + id + ']';
    }
}
