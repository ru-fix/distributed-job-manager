package ru.fix.distributed.job.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.distributed.job.manager.util.DistributedJobSettings;
import ru.fix.dynamic.property.api.DynamicProperty;

import java.util.Collection;

public class DistributedJobManagerConfigHelper {
    private static final Logger log = LoggerFactory.getLogger(DistributedJobManagerConfigHelper.class);

    public static DynamicProperty<DistributedJobSettings> toRunWith(boolean enabled, Collection<?> collection) {
        if (enabled) {
            log.info("All jobs 'enabled' status :  TRUE");
            return allJobsEnabledTrue(collection);
        } else {
            log.info("All jobs 'enabled' status : FALSE");
            return allJobsEnabledFalse(collection);
        }
    }

    protected static DynamicProperty<DistributedJobSettings> allJobsEnabledTrue(Collection<?> collection) {
        DistributedJobSettings distributedJobSettings = new DistributedJobSettings();
        for (Object dj : collection) {
            distributedJobSettings.addConfig(((DistributedJob) dj).getJobId(), true);
        }

        return DynamicProperty.of(distributedJobSettings);
    }


    protected static DynamicProperty<DistributedJobSettings> allJobsEnabledFalse(Collection<?> collection) {
        DistributedJobSettings distributedJobSettings = new DistributedJobSettings();
        for (Object dj : collection) {
            distributedJobSettings.addConfig(((DistributedJob) dj).getJobId(), false);
        }

        return DynamicProperty.of(distributedJobSettings);
    }
}
