package ru.fix.distributed.job.manager

import io.mockk.spyk
import io.mockk.verify
import org.apache.curator.framework.CuratorFramework
import org.junit.jupiter.api.Test
import ru.fix.aggregating.profiler.NoopProfiler
import ru.fix.aggregating.profiler.Profiler
import ru.fix.distributed.job.manager.model.DistributedJobManagerSettings
import ru.fix.distributed.job.manager.model.JobDisableConfig
import ru.fix.distributed.job.manager.strategy.AssignmentStrategies
import ru.fix.distributed.job.manager.strategy.AssignmentStrategy
import ru.fix.dynamic.property.api.AtomicProperty

private const val defaultJobRunTimeoutMs = 2_000L

internal class JobManagerSettingsIT : AbstractJobManagerTest() {

    @Test
    fun `WHEN disableAllJobsProperty changed THEN jobs running accordingly`() {
        val job1 = spyk(createStubbedJob(1))
        val job2 = spyk(createStubbedJob(2))
        val settingsEditor = JobManagerSettingsEditor()
        settingsEditor.setDisableAllJobProperty(true)
        createDjm(
                settingsEditor = settingsEditor,
                jobs = listOf(job1, job2)
        ).use {
            Thread.sleep(defaultJobRunTimeoutMs)
            verify(exactly = 0) {
                job1.run(any())
                job2.run(any())
            }
            settingsEditor.setDisableAllJobProperty(false)
            verify(exactly = 1, timeout = defaultJobRunTimeoutMs) {
                job1.run(any())
                job2.run(any())
            }
            settingsEditor.setDisableAllJobProperty(true)
            Thread.sleep(defaultJobRunTimeoutMs)
            verify(exactly = 1) {
                job1.run(any())
                job2.run(any())
            }
            settingsEditor.setDisableAllJobProperty(false)
            verify(atLeast = 2, timeout = defaultJobRunTimeoutMs) {
                job1.run(any())
                job2.run(any())
            }
        }
    }

    @Test
    fun `WHEN jobs disable switches changed THEN jobs running accordingly`() {
        val job1 = spyk(createStubbedJob(1))
        val job2 = spyk(createStubbedJob(2))
        val settingsEditor = JobManagerSettingsEditor()
        settingsEditor.disableConcreteJob(job1)
        createDjm(
                settingsEditor = settingsEditor,
                jobs = listOf(job1, job2)
        ).use {
            verify(exactly = 1, timeout = defaultJobRunTimeoutMs) { job2.run(any()) }
            verify(exactly = 0) { job1.run(any()) }
            settingsEditor.disableConcreteJob(job2)
            Thread.sleep(defaultJobRunTimeoutMs)
            verify(exactly = 1) { job2.run(any()) }
            verify(exactly = 0) { job1.run(any()) }
            settingsEditor.enableConcreteJob(job1)
            verify(exactly = 1, timeout = defaultJobRunTimeoutMs) {
                job1.run(any())
                job2.run(any())
            }
            settingsEditor.enableConcreteJob(job2)
            verify(atLeast = 2, timeout = defaultJobRunTimeoutMs) {
                job1.run(any())
                job2.run(any())
            }
        }
    }

    @Test
    fun `WHEN disableAllJobsProperty is true THEN jobs switches don't matter`() {
        val job1 = spyk(createStubbedJob(1))
        val job2 = spyk(createStubbedJob(2))
        val settingsEditor = JobManagerSettingsEditor().apply {
            setDisableAllJobProperty(true)
            enableConcreteJob(job1)
            setDisableJobDefaultValue(false)
        }
        createDjm(
                settingsEditor = settingsEditor,
                jobs = listOf(job1, job2)
        ).use {
            Thread.sleep(defaultJobRunTimeoutMs)
            verify(exactly = 0) {
                job1.run(any())
                job2.run(any())
            }
        }
    }

    private fun createStubbedJob(
            jobId: Int = 1,
            workItems: Set<String> = setOf("1", "2"),
            delay: Long = 500,
            workPoolCheckPeriod: Long = 0
    ): StubbedMultiJob = StubbedMultiJob(
            jobId, workItems, delay, workPoolCheckPeriod
    )


    private fun createDjm(
            settingsEditor: JobManagerSettingsEditor = JobManagerSettingsEditor(),
            jobs: Collection<DistributedJob>,
            curatorFramework: CuratorFramework = zkTestingServer.createClient(60000, 15000),
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
        setJobIsDisabled(job.jobId, true)
    }

    fun enableConcreteJob(job: DistributedJob) {
        setJobIsDisabled(job.jobId, false)
    }

    private fun setJobIsDisabled(jobId: JobId, value: Boolean) {
        val oldConfig = jobDisableConfig.get()
        val newSwitches = HashMap(oldConfig.jobsDisableSwitches).apply {
            this[jobId.id] = value
        }
        jobDisableConfig.set(oldConfig.copy(jobsDisableSwitches = newSwitches))
    }
}