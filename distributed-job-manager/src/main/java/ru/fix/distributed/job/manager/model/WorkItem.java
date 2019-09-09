package ru.fix.distributed.job.manager.model;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public class WorkItem {

    private String id;
    private String jobId;

    public WorkItem(String id, String jobId) {
        Objects.requireNonNull(id);
        this.id = id;
        this.jobId = jobId;
    }

    public WorkItem(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public String getJobId() {
        return jobId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WorkItem workItem = (WorkItem) o;
        return Objects.equals(id, workItem.id) &&
                Objects.equals(jobId, workItem.jobId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, jobId);
    }

    @Override
    public String toString() {
        return "WorkItem{" +
                "id='" + id + '\'' +
                ", jobId='" + jobId + '\'' +
                '}';
    }
}
