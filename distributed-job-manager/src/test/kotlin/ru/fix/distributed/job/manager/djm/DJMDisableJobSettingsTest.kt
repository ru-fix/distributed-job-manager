package ru.fix.distributed.job.manager.djm

import com.nhaarman.mockitokotlin2.*
import org.apache.curator.framework.CuratorFramework
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import ru.fix.aggregating.profiler.NoopProfiler
import ru.fix.aggregating.profiler.Profiler
import ru.fix.distributed.job.manager.*
import ru.fix.distributed.job.manager.model.DistributedJobManagerSettings
import ru.fix.distributed.job.manager.model.JobDisableConfig
import ru.fix.distributed.job.manager.model.JobIdResolver.resolveJobId
import ru.fix.dynamic.property.api.AtomicProperty
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean


class DJMDisableJobSettingsTest : DJMTestSuite() {

    companion object {
        private val defaultJobRunTimeout = Duration.ofSeconds(2)
    }

    open class FrequentJob(jobId: Int): DistributedJob {
        val launched = AtomicBoolean()

        override val jobId = JobId("FrequentJob-$jobId")
        override fun getSchedule(): DynamicProperty<Schedule> = DynamicProperty.of(Schedule.withRate(500))
        override fun run(context: DistributedJobContext) { launched.set(true) }
        override fun getWorkPool() = WorkPool.of("1", "2")
        override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
        override fun getWorkPoolCheckPeriod(): Long = 0
    }

    @Test
    fun `WHEN disableAllJobsProperty changed THEN jobs running accordingly`() {
        val job1 = spy(FrequentJob(1))
        val job2 = spy(FrequentJob(2))
        val settingsEditor = JobManagerSettingsEditor()

        settingsEditor.setDisableAllJobProperty(true)

        createDjm(
                settingsEditor = settingsEditor,
                jobs = listOf(job1, job2)
        ).use {
            await().pollDelay(defaultJobRunTimeout).untilAsserted {
                verify(job1, never()).run(any())
                verify(job2, never()).run(any())
            }
            settingsEditor.setDisableAllJobProperty(false)
            await().atMost(defaultJobRunTimeout).untilAsserted {
                verify(job1, times(1)).run(any())
                verify(job2, times(1)).run(any())
            }
            settingsEditor.setDisableAllJobProperty(true)
            await().pollDelay(defaultJobRunTimeout).untilAsserted {
                verify(job1, times(1)).run(any())
                verify(job2, times(1)).run(any())
            }
            settingsEditor.setDisableAllJobProperty(false)
            await().atMost(defaultJobRunTimeout).untilAsserted {
                verify(job1, atLeast(2)).run(any())
                verify(job2, atLeast(2)).run(any())
            }
        }
    }

    @Test
    fun `WHEN jobs disable switches changed THEN jobs running accordingly`() {
        val job1 = spy(FrequentJob(1))
        val job2 = spy(FrequentJob(2))
        val settingsEditor = JobManagerSettingsEditor()
        settingsEditor.disableConcreteJob(job1)
        createDjm(
                settingsEditor = settingsEditor,
                jobs = listOf(job1, job2)
        ).use {
            await().atMost(defaultJobRunTimeout).untilAsserted {
                verify(job1, never()).run(any())
                verify(job2, times(1)).run(any())
            }
            settingsEditor.disableConcreteJob(job2)
            await().pollDelay(defaultJobRunTimeout).untilAsserted {
                verify(job1, never()).run(any())
                verify(job2, times(1)).run(any())
            }
            settingsEditor.enableConcreteJob(job1)
            await().atMost(defaultJobRunTimeout).untilAsserted {
                verify(job1, times(1)).run(any())
                verify(job2, times(1)).run(any())
            }
            settingsEditor.enableConcreteJob(job2)
            await().atMost(defaultJobRunTimeout).untilAsserted {
                verify(job1, atLeast(2)).run(any())
                verify(job2, atLeast(2)).run(any())
            }
        }
    }

    @Test
    fun `WHEN disableAllJobsProperty is true THEN jobs switches don't matter`() {
        val job1 = spy(FrequentJob(1))
        val job2 = spy(FrequentJob(2))
        val settingsEditor = JobManagerSettingsEditor().apply {
            setDisableAllJobProperty(true)
            enableConcreteJob(job1)
            setDisableJobDefaultValue(false)
        }
        createDjm(
                settingsEditor = settingsEditor,
                jobs = listOf(job1, job2)
        ).use {
            await().pollDelay(defaultJobRunTimeout).untilAsserted {
                verify(job1, never()).run(any())
                verify(job2, never()).run(any())
            }
        }
    }

    private fun createDjm(
            settingsEditor: JobManagerSettingsEditor = JobManagerSettingsEditor(),
            jobs: Collection<DistributedJob>,
            curatorFramework: CuratorFramework = server.client,
            profiler: Profiler = NoopProfiler()
    ) = DistributedJobManager(
            curatorFramework,
            jobs,
            profiler,
            settingsEditor.toSettings()
    )
}

private class JobManagerSettingsEditor(
        initialTimeToWaitTermination: Long = 180_000,
        initialJobDisableConfig: JobDisableConfig = JobDisableConfig()
) {
    private val jobDisableConfig: AtomicProperty<JobDisableConfig> = AtomicProperty(initialJobDisableConfig)

    fun toSettings() = DistributedJobManagerSettings(
            nodeId = "1",
            rootPath = DJMDisableJobSettingsTest.javaClass.simpleName,
            jobDisableConfig = jobDisableConfig)

    fun setDisableAllJobProperty(disableAll: Boolean) {
        jobDisableConfig.set(jobDisableConfig.get().copy(disableAllJobs = disableAll))
    }

    fun setDisableJobDefaultValue(disabledByDefault: Boolean) {
        jobDisableConfig.set(jobDisableConfig.get().copy(defaultDisableJobSwitchValue = disabledByDefault))
    }

    fun disableConcreteJob(job: DistributedJob) {
        setJobIsDisabled(resolveJobId(job), true)
    }

    fun enableConcreteJob(job: DistributedJob) {
        setJobIsDisabled(resolveJobId(job), false)
    }

    private fun setJobIsDisabled(jobId: JobId, value: Boolean) {
        val oldConfig = jobDisableConfig.get()
        val newSwitches = HashMap(oldConfig.jobsDisableSwitches).apply {
            this[jobId.id] = value
        }
        jobDisableConfig.set(oldConfig.copy(jobsDisableSwitches = newSwitches))
    }
}