package ru.fix.distributed.job.manager.model

// Map is immutable.
data class DistributedJobSettings(val timeToWaitTermination: Long = 0L,
                                  var jobsEnabledStatus: MutableMap<String, Boolean> = mutableMapOf())