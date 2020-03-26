package ru.fix.distributed.job.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.distributed.job.manager.util.DistributedJobSettings;
import ru.fix.dynamic.property.api.DynamicProperty;

import java.util.Collection;
import java.util.HashMap;

public class DistributedJobManagerConfigHelper {
    private static final Logger log = LoggerFactory.getLogger(DistributedJobManagerConfigHelper.class);

    public DynamicProperty<DistributedJobSettings> toRunWith(int state, Collection<?> collection) {
        switch (state) {
            case 1:
                log.info("All jobs 'enabled' status : FALSE");
                return allJobsEnabledFalse(collection);
            case 2:
                log.info("All jobs 'enabled' status :  TRUE");
                return allJobsEnabledTrue(collection);
            case 3:
                log.info("All jobs 'enabled' status : ");
                return randomJobsEnabled(collection);
        }
        throw new Error();
    }

    protected DynamicProperty<DistributedJobSettings> allJobsEnabledTrue(Collection<?> collection) {
        DistributedJobSettings DJS = new DistributedJobSettings();
        for (Object dj : collection) {
            DJS.addConfig(((DistributedJob) dj).getJobId(), true);
        }

        return DynamicProperty.of(DJS);
    }


    protected DynamicProperty<DistributedJobSettings> allJobsEnabledFalse(Collection<?> collection) {
        DistributedJobSettings DJS = new DistributedJobSettings();
        for (Object dj : collection) {
            DJS.addConfig(((DistributedJob) dj).getJobId(), false);
        }

        return DynamicProperty.of(DJS);
    }

    protected DynamicProperty<DistributedJobSettings> randomJobsEnabled(Collection<?> collection) {
        DistributedJobSettings DJS = new DistributedJobSettings(new HashMap<>());
        for (Object dj : collection) {
            DJS.addConfig(((DistributedJob)dj).getJobId(), randomBoolean());
        }
        return DynamicProperty.of(DJS);
    }

    private boolean randomBoolean() {
        return Math.random() < 0.5;
    }
}
