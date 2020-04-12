package ru.fix.distributed.job.manager.model

import ru.fix.distributed.job.manager.util.DistributedJobSettings
import ru.fix.dynamic.property.api.DynamicProperty

data class DistributedJobsPreset(val timeToWaitTermination: DynamicProperty<Long>,
                                 val jobsEnabledStatus: DynamicProperty<DistributedJobSettings>)