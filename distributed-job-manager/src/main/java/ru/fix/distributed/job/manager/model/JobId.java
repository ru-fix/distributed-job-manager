package ru.fix.distributed.job.manager.model;

import java.util.Objects;

public class JobId {

    private String id;

    public JobId(String id) {
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
        JobId jobId = (JobId) o;
        return id.equals(jobId.id);
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
