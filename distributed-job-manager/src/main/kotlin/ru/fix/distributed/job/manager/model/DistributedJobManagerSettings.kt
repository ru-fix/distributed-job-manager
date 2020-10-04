package ru.fix.distributed.job.manager.model

import ru.fix.zookeeper.lock.PersistentExpiringLockManagerConfig
import java.time.Duration

data class DistributedJobManagerSettings @JvmOverloads constructor(

        /**
         * Time to wait for tasks to complete or redistributed when the DJM instance is closing
         * */
        val timeToWaitTermination: Duration = Duration.ofMinutes(15),
        /**
         * How often cleaning process checks for obsolete not relevant jobs
         * in ZooKeeper `work-pool` subtree.
         * Obsolete jobs appears when new version of application with different set of jobs is starts in the cluster.
         * Minor process. Default value is three hours
         * */
        val workPoolCleanPeriod: Duration = Duration.ofHours(3),

        /**
         * Disabled Job work items continue to distribute and assign but job stop launching.
         */
        val jobDisableConfig: JobDisableConfig = JobDisableConfig(),

        /**
         * Policy for persistent locks used to prevent parallel execution of same work item wihin DJM cluster.
         */
        val lockManagerConfig: PersistentExpiringLockManagerConfig = PersistentExpiringLockManagerConfig()
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
