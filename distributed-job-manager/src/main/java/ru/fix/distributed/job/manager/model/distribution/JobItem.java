package ru.fix.distributed.job.manager.model.distribution;

import java.util.Objects;

public class JobItem {

    private String id;

    public JobItem(String id) {
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
        JobItem jobItem = (JobItem) o;
        return id.equals(jobItem.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return "Job[" + id + ']';
    }
}
