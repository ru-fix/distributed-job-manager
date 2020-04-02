package ru.fix.distributed.job.manager.util;

import java.util.HashMap;

public class DistributedJobSettings {
    private HashMap<String, Boolean> jobsEnabled;

    public DistributedJobSettings() {
        jobsEnabled = new HashMap<>();
    }

    public DistributedJobSettings(HashMap<String, Boolean> jobsEnabled) {
        this.jobsEnabled = jobsEnabled;
    }

    public void addConfig(String jobId, Boolean enabled) {
        jobsEnabled.put(jobId, enabled);
    }

    public boolean getJobProperty(String jobId) {
        return jobsEnabled.get(jobId);
    }
}

