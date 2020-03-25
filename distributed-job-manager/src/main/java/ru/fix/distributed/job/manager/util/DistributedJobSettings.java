package ru.fix.distributed.job.manager.util;

import java.util.HashMap;

public class DistributedJobSettings{
    private HashMap<String, Boolean> jobsEnabled;

    public DistributedJobSettings(){
        jobsEnabled = new HashMap<>();
    }
    public DistributedJobSettings(HashMap<String, Boolean> jobsEnabled){
        this.jobsEnabled = jobsEnabled;
    }

    public void addConfig(String jobId, boolean enabled) {
        jobsEnabled.put(jobId, enabled);
    }

    public boolean getEnabled(String jobId) {
        return jobsEnabled.getOrDefault(jobId, false);
    }
}

