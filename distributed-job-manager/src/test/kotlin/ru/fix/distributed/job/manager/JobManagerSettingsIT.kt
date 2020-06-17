package ru.fix.distributed.job.manager

import org.apache.curator.framework.CuratorFramework
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.*
import ru.fix.aggregating.profiler.NoopProfiler
import ru.fix.aggregating.profiler.Profiler
import ru.fix.distributed.job.manager.model.DistributedJobManagerSettings
import ru.fix.distributed.job.manager.model.JobDisableConfig
import ru.fix.distributed.job.manager.strategy.AssignmentStrategies
import ru.fix.distributed.job.manager.strategy.AssignmentStrategy
import ru.fix.dynamic.property.api.AtomicProperty
import java.time.Duration


internal class JobManagerSettingsIT : AbstractJobManagerTest() {

    private val defaultJobRunTimeoutMs = 1_000L

    @Test
    fun `WHEN disableAllJobsProperty changed THEN jobs running accordingly`() {
        val job1 = spy(createStubbedJob(1))
        val job2 = spy(createStubbedJob(2))
        val settingsEditor = JobManagerSettingsEditor()
        settingsEditor.setDisableAllJobProperty(true)
        createDjm(
                settingsEditor = settingsEditor,
                jobs = listOf(job1, job2)
        ).use {
            await().pollDelay(Duration.ofMillis(defaultJobRunTimeoutMs)).untilAsserted {
                verify(job1, never()).run(any())
                verify(job2, never()).run(any())
            }
            settingsEditor.setDisableAllJobProperty(false)
            await().atMost(Duration.ofMillis(defaultJobRunTimeoutMs)).untilAsserted {
                verify(job1, times(1)).run(any())
                verify(job2, times(1)).run(any())
            }
            settingsEditor.setDisableAllJobProperty(true)
            await().pollDelay(Duration.ofMillis(defaultJobRunTimeoutMs)).untilAsserted {
                verify(job1, times(1)).run(any())
                verify(job2, times(1)).run(any())
            }
            settingsEditor.setDisableAllJobProperty(false)
            await().atMost(Duration.ofMillis(defaultJobRunTimeoutMs)).untilAsserted {
                verify(job1, times(2)).run(any())
                verify(job2, times(2)).run(any())
            }
        }
    }

    @Test
    fun `WHEN jobs disable switches changed THEN jobs running accordingly`() {
        val job1 = spy(createStubbedJob(1))
        val job2 = spy(createStubbedJob(2))
        val settingsEditor = JobManagerSettingsEditor()
        settingsEditor.disableConcreteJob(job1)
        createDjm(
                settingsEditor = settingsEditor,
                jobs = listOf(job1, job2)
        ).use {
            await().atMost(Duration.ofMillis(defaultJobRunTimeoutMs)).untilAsserted {
                verify(job1, never()).run(any())
                verify(job2, times(1)).run(any())
            }
            settingsEditor.disableConcreteJob(job2)
            await().pollDelay(Duration.ofMillis(defaultJobRunTimeoutMs)).untilAsserted {
                verify(job1, never()).run(any())
                verify(job2, times(1)).run(any())
            }
            settingsEditor.enableConcreteJob(job1)
            await().atMost(Duration.ofMillis(defaultJobRunTimeoutMs)).untilAsserted {
                verify(job1, times(1)).run(any())
                verify(job2, times(1)).run(any())
            }
            settingsEditor.enableConcreteJob(job2)
            await().atMost(Duration.ofMillis(defaultJobRunTimeoutMs)).untilAsserted {
                verify(job1, times(2)).run(any())
                verify(job2, times(2)).run(any())
            }
        }
    }

    @Test
    fun `WHEN disableAllJobsProperty is true THEN jobs switches don't matter`() {
        val job1 = spy(createStubbedJob(1))
        val job2 = spy(createStubbedJob(2))
        val settingsEditor = JobManagerSettingsEditor().apply {
            setDisableAllJobProperty(true)
            enableConcreteJob(job1)
            setDisableJobDefaultValue(false)
        }
        createDjm(
                settingsEditor = settingsEditor,
                jobs = listOf(job1, job2)
        ).use {
            await().pollDelay(Duration.ofMillis(defaultJobRunTimeoutMs)).untilAsserted {
                verify(job1, never()).run(any())
                verify(job2, never()).run(any())
            }
        }
    }

    private fun createStubbedJob(
            jobId: Int = 1,
            workItems: Set<String> = setOf("1", "2"),
            delay: Long = 100,
            workPoolCheckPeriod: Long = 0
    ): StubbedMultiJob = StubbedMultiJob(
            jobId, workItems, delay, workPoolCheckPeriod
    )


    private fun createDjm(
            settingsEditor: JobManagerSettingsEditor = JobManagerSettingsEditor(),
            jobs: Collection<DistributedJob>,
            curatorFramework: CuratorFramework = zkTestingServer.createClient(),
            profiler: Profiler = NoopProfiler()
    ) = DistributedJobManager(
            curatorFramework,
            jobs,
            profiler,
            settingsEditor.toSettings()
    )
}

private class JobManagerSettingsEditor(
        val nodeId: String = "1",
        val rootPath: String = "DistributedJobConfigIT",
        val assignmentStrategy: AssignmentStrategy = AssignmentStrategies.DEFAULT,
        initialTimeToWaitTermination: Long = 180_000,
        initialJobDisableConfig: JobDisableConfig = JobDisableConfig()
) {
    private val timeToWaitTermination: AtomicProperty<Long> = AtomicProperty(initialTimeToWaitTermination)
    private val jobDisableConfig: AtomicProperty<JobDisableConfig> = AtomicProperty(initialJobDisableConfig)

    fun toSettings() = DistributedJobManagerSettings(
            nodeId = nodeId,
            rootPath = rootPath,
            assignmentStrategy = assignmentStrategy,
            timeToWaitTermination = timeToWaitTermination,
            jobDisableConfig = jobDisableConfig
    )

    fun setDisableAllJobProperty(disableAll: Boolean) {
        jobDisableConfig.set(jobDisableConfig.get().copy(disableAllJobs = disableAll))
    }

    fun setDisableJobDefaultValue(disabledByDefault: Boolean) {
        jobDisableConfig.set(jobDisableConfig.get().copy(defaultDisableJobSwitchValue = disabledByDefault))
    }

    fun disableConcreteJob(job: DistributedJob) {
        setJobIsDisabled(job.getJobId(), true)
    }

    fun enableConcreteJob(job: DistributedJob) {
        setJobIsDisabled(job.getJobId(), false)
    }

    private fun setJobIsDisabled(jobId: String, value: Boolean) {
        val oldConfig = jobDisableConfig.get()
        val newSwitches = HashMap(oldConfig.jobsDisableSwitches).apply {
            this[jobId] = value
        }
        jobDisableConfig.set(oldConfig.copy(jobsDisableSwitches = newSwitches))
    }
}