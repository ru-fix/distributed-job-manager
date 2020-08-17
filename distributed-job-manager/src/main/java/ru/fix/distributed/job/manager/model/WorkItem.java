package ru.fix.distributed.job.manager.model;

import kotlin.text.Regex;
import ru.fix.distributed.job.manager.JobId;

import java.util.Objects;

public class WorkItem {
    private static final Regex PATTERN = new Regex("[a-zA-Z0-9_-[.]]+");
    private final String id;
    private final JobId jobId;

    public WorkItem(String id, JobId jobId) {
        Objects.requireNonNull(id);
        Objects.requireNonNull(jobId);
        if (!PATTERN.matches(id)) {
            throw new IllegalArgumentException("Work item's id should matches pattern " + PATTERN);
        }
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