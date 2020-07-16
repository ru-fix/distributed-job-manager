package ru.fix.distributed.job.manager.model

import ru.fix.distributed.job.manager.strategy.AssignmentStrategies
import ru.fix.distributed.job.manager.strategy.AssignmentStrategy
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.zookeeper.lock.PersistentExpiringLockManagerConfig

data class DistributedJobManagerSettings @JvmOverloads constructor(
        val nodeId: String,
        val rootPath: String,
        val assignmentStrategy: AssignmentStrategy = AssignmentStrategies.DEFAULT,
        /**
         * Time to wait for tasks to be completed when the application is closed and when tasks are redistributed
         * */
        val timeToWaitTermination: DynamicProperty<Long>,
        /**
         * Delay between launching task for removing not relevant jobs from `work-pool` subtree.
         * Minor process. Default value is three hours
         * */
        val workPoolCleanPeriod: DynamicProperty<Long> = DynamicProperty.of(10_800_000),

        val jobDisableConfig: DynamicProperty<JobDisableConfig> = DynamicProperty.of(JobDisableConfig()),

        val lockManagerConfig: DynamicProperty<PersistentExpiringLockManagerConfig> =
                DynamicProperty.of(PersistentExpiringLockManagerConfig())
)

data class JobDisableConfig(
        /**
         * * False: djm works normally, all jobs are enabled or disabled
         * according to [jobsDisableSwitches] and [defaultDisableJobSwitchValue]
         * * True: all jobs are disabled
         * */
        val disableAllJobs: Boolean = false,
        /**
         * If jobId is not contained in [jobsDisableSwitches]
         * then job is enabled or disabled according to this value:
         * * True = disabled;
         * * False = enabled.
         * */
        val defaultDisableJobSwitchValue: Boolean = false,
        /**
         * Map<JobId, DisabledBoolean>
         * * True value: job with key id is disabled
         * * False value: job with key id will be launched if [disableAllJobs] is false
         * * Absence of jobId among the keys means [defaultDisableJobSwitchValue]
         * */
        val jobsDisableSwitches: Map<String, Boolean> = emptyMap()
) {
    /**
     * @return true if job will be launched by worker according to this whole config, or false otherwise
     * */
    fun isJobShouldBeLaunched(jobId: String): Boolean = !disableAllJobs && !isJobIsDisabledBySwitch(jobId)

    /**
     * @return
     * * true if [defaultDisableJobSwitchValue] and [jobsDisableSwitches]
     * configured so that the job with the given id is disabled regardless of [disableAllJobs] value
     *
     * * false if [defaultDisableJobSwitchValue] and [jobsDisableSwitches]
     * configured so that the job with the given id will be launched if [disableAllJobs] is false
     * */
    fun isJobIsDisabledBySwitch(jobId: String): Boolean = jobsDisableSwitches[jobId] ?: defaultDisableJobSwitchValue
}
