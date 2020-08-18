package ru.fix.distributed.job.manager.model;

import ru.fix.distributed.job.manager.IdentityValidator;
import ru.fix.distributed.job.manager.JobId;

import java.util.Objects;

public class WorkItem {
    private final String id;
    private final JobId jobId;

    public WorkItem(String id, JobId jobId) {
        IdentityValidator.validate(IdentityValidator.IdentityType.WorkItem, id);
        Objects.requireNonNull(jobId);
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
        return "WorkItem[job=" + jobId.getId() + ", id=" + id + "]";
    }
}
