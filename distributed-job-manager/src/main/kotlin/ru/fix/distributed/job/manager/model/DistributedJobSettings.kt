package ru.fix.distributed.job.manager.model

import java.time.Duration

data class DistributedJobSettings(val timeToWaitTermination: Long = Duration.ofSeconds(30).seconds,
                                  var jobsEnabledStatus: MutableMap<String, Boolean> = mutableMapOf())