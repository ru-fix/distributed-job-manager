package ru.fix.distributed.job.manager.model

import kotlin.time.ExperimentalTime
import kotlin.time.seconds

@ExperimentalTime
data class DistributedJobSettings(val timeToWaitTermination: Long = 30.seconds.toLongMilliseconds(),
                                  var jobsEnabledStatus: MutableMap<String, Boolean> = mutableMapOf())