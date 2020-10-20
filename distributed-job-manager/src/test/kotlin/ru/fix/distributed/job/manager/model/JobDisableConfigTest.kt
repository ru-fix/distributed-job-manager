package ru.fix.distributed.job.manager.model

import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

internal class JobDisableConfigTest {

    @Test
    fun `isJobShouldBeLaunched WHEN disableAllJobs is true THEN returns false`() {
        assertFalse(
            JobDisableConfig(disableAllJobs = true, defaultDisableJobSwitchValue = true)
                .isJobShouldBeLaunched("any job")
        )
        assertFalse(
            JobDisableConfig(disableAllJobs = true, defaultDisableJobSwitchValue = false)
                .isJobShouldBeLaunched("any job")
        )
        assertFalse(
            JobDisableConfig(disableAllJobs = true, jobsDisableSwitches = mapOf("job" to true))
                .isJobShouldBeLaunched("job")
        )
    }

    @Test
    fun `WHEN jobs are enabled by default AND job is not contained in switches THEN is not disabled by switches AND should be launched`() {
        val config = JobDisableConfig(defaultDisableJobSwitchValue = false)
        assertFalse(config.isJobIsDisabledBySwitch("job"))
        assertTrue(config.isJobShouldBeLaunched("job"))
    }

    @Test
    fun `WHEN jobs are disabled by default AND job is not contained in switches THEN is disabled by switches AND shouldn't be launched`() {
        val config = JobDisableConfig(defaultDisableJobSwitchValue = true)
        assertTrue(config.isJobIsDisabledBySwitch("job"))
        assertFalse(config.isJobShouldBeLaunched("job"))
    }

    @Test
    fun `WHEN job is contained in switches THEN defaultDisableJobSwitchValue doesn't matter`() {
        val jobName = "sms.job"

        with(JobDisableConfig(defaultDisableJobSwitchValue = false, jobsDisableSwitches = mapOf(jobName to true))) {
            assertTrue(isJobIsDisabledBySwitch(jobName))
            assertFalse(isJobShouldBeLaunched(jobName))
        }
        with(JobDisableConfig(defaultDisableJobSwitchValue = true, jobsDisableSwitches = mapOf(jobName to true))) {
            assertTrue(isJobIsDisabledBySwitch(jobName))
            assertFalse(isJobShouldBeLaunched(jobName))
        }
        with(JobDisableConfig(defaultDisableJobSwitchValue = false, jobsDisableSwitches = mapOf(jobName to false))) {
            assertFalse(isJobIsDisabledBySwitch(jobName))
            assertTrue(isJobShouldBeLaunched(jobName))
        }
        with(JobDisableConfig(defaultDisableJobSwitchValue = true, jobsDisableSwitches = mapOf(jobName to false))) {
            assertFalse(isJobIsDisabledBySwitch(jobName))
            assertTrue(isJobShouldBeLaunched(jobName))
        }

    }
}