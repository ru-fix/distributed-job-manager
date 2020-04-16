package ru.fix.distributed.job.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.distributed.job.manager.util.DistributedJobSettings;
import ru.fix.dynamic.property.api.DynamicProperty;

import java.util.Collection;

public class DistributedJobManagerConfigHelper {
    private static final Logger log = LoggerFactory.getLogger(DistributedJobManagerConfigHelper.class);

    protected static DynamicProperty<DistributedJobSettings> allJobsEnabledTrue(Collection<DistributedJob> collection) {
        DistributedJobSettings distributedJobSettings = new DistributedJobSettings();
        for (Object dj : collection) {
            distributedJobSettings.addConfig(((DistributedJob) dj).getJobId(), true);
        }

        return DynamicProperty.of(distributedJobSettings);
    }


    protected static DynamicProperty<DistributedJobSettings> allJobsEnabledFalse(Collection<DistributedJob> collection) {
        DistributedJobSettings distributedJobSettings = new DistributedJobSettings();
        for (Object dj : collection) {
            distributedJobSettings.addConfig(((DistributedJob) dj).getJobId(), false);
        }

        return DynamicProperty.of(distributedJobSettings);
    }
}
