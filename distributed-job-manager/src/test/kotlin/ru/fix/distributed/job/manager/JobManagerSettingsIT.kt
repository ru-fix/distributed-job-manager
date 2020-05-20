package ru.fix.distributed.job.manager

import org.apache.curator.framework.CuratorFramework
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.*
import ru.fix.aggregating.profiler.NoopProfiler
import ru.fix.aggregating.profiler.Profiler
import ru.fix.distributed.job.manager.model.DistributedJobManagerSettings
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
        val settingsEditor = JobManagerSettingsEditor(allJobDisabledPropertyInitialValue = true)
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
        allJobDisabledPropertyInitialValue: Boolean = false
) {
    private val timeToWaitTermination: AtomicProperty<Long> = AtomicProperty(initialTimeToWaitTermination)
    private val disableAllJobsProperty: AtomicProperty<Boolean> = AtomicProperty(allJobDisabledPropertyInitialValue)

    fun toSettings() = DistributedJobManagerSettings(
            nodeId = nodeId,
            rootPath = rootPath,
            assignmentStrategy = assignmentStrategy,
            timeToWaitTermination = timeToWaitTermination,
            disableAllJobs = disableAllJobsProperty
    )

    fun setDisableAllJobProperty(value: Boolean): Boolean = disableAllJobsProperty.set(value)
}