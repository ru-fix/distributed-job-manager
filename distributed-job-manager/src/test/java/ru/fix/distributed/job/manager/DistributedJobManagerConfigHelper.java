package ru.fix.distributed.job.manager;

import ru.fix.distributed.job.manager.model.DistributedJobSettings;
import ru.fix.dynamic.property.api.DynamicProperty;

import java.util.Collection;

public class DistributedJobManagerConfigHelper {
    public static DynamicProperty<DistributedJobSettings> allJobsEnabled(Collection<DistributedJob> jobs) {
        return allJobs(true, jobs);
    }


    public static DynamicProperty<DistributedJobSettings> allJobsDisabled(Collection<DistributedJob> jobs) {
        return allJobs(false, jobs);
    }


    private static DynamicProperty<DistributedJobSettings> allJobs(boolean enabled,
                                                                   Collection<DistributedJob> jobs) {
        DistributedJobSettings distributedJobSettings = new DistributedJobSettings();
        for (DistributedJob dj : jobs) {
            distributedJobSettings.getJobsEnabledStatus().put(dj.getJobId(), enabled);
        }
        return DynamicProperty.of(distributedJobSettings);
    }
}
