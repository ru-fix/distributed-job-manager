package ru.fix.distributed.job.manager.model;

import java.util.Objects;

public class WorkItem {

    private String id;
    private JobId jobId;

    public WorkItem(String id, JobId jobId) {
        Objects.requireNonNull(id);
        this.id = id;
        this.jobId = jobId;
    }

    public String getId() {
        return id;
    }

    public JobId getJobId() {
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
