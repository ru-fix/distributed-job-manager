package ru.fix.distributed.job.manager.djm

import com.nhaarman.mockitokotlin2.*
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import ru.fix.distributed.job.manager.*
import ru.fix.distributed.job.manager.model.DistributedJobManagerSettings
import ru.fix.distributed.job.manager.model.JobDisableConfig
import ru.fix.distributed.job.manager.model.JobIdResolver.resolveJobId
import ru.fix.dynamic.property.api.AtomicProperty
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule
import ru.fix.zookeeper.lock.PersistentExpiringLockManagerConfig
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean


class DJMDisableJobSettingsTest : DJMTestSuite() {

    companion object {
        private val defaultJobRunTimeout = Duration.ofSeconds(2)
    }

    open class FrequentJob(jobId: Int) : DistributedJob {
        val launched = AtomicBoolean()

        override val jobId = JobId("FrequentJob-$jobId")
        override fun getSchedule(): DynamicProperty<Schedule> = DynamicProperty.of(Schedule.withRate(500))
        override fun run(context: DistributedJobContext) {
            launched.set(true)
        }

        override fun getWorkPool() = WorkPool.of("1", "2")
        override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
        override fun getWorkPoolCheckPeriod(): Long = 0
    }

    @Test
    fun `WHEN disableAllJobsProperty changed THEN jobs running accordingly`() {
        val job1 = spy(FrequentJob(1))
        val job2 = spy(FrequentJob(2))
        val settingsEditor = JobDisableConfigEditor()

        settingsEditor.setDisableAllJobProperty(true)

        val djm = createDJM(
                jobs = listOf(job1, job2),
                jobDisableConfig = settingsEditor.jobDisableConfig
        )

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

    @Test
    fun `WHEN jobs disable switches changed THEN jobs running accordingly`() {
        val job1 = spy(FrequentJob(1))
        val job2 = spy(FrequentJob(2))
        val settingsEditor = JobDisableConfigEditor()
        settingsEditor.disableConcreteJob(job1)
        val djm = createDJM(
                jobs = listOf(job1, job2),
                jobDisableConfig = settingsEditor.jobDisableConfig
        )
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

    @Test
    fun `WHEN disableAllJobsProperty is true THEN jobs switches don't matter`() {
        val job1 = spy(FrequentJob(1))
        val job2 = spy(FrequentJob(2))
        val settingsEditor = JobDisableConfigEditor().apply {
            setDisableAllJobProperty(true)
            enableConcreteJob(job1)
            setDisableJobDefaultValue(false)
        }
        val djm = createDJM(
                jobs = listOf(job1, job2),
                jobDisableConfig = settingsEditor.jobDisableConfig
        )
        await().pollDelay(defaultJobRunTimeout).untilAsserted {
            verify(job1, never()).run(any())
            verify(job2, never()).run(any())
        }

    }

    fun createDJM(jobs: List<DistributedJob>, jobDisableConfig: DynamicProperty<JobDisableConfig>) =
            createDJM(
                    jobs = jobs,
                    settings = jobDisableConfig.map {
                        DistributedJobManagerSettings(
                                timeToWaitTermination = 10000,
                                workPoolCleanPeriod = 1000,
                                lockManagerConfig = PersistentExpiringLockManagerConfig(
                                        lockAcquirePeriod = Duration.ofSeconds(15),
                                        expirationPeriod = Duration.ofSeconds(5),
                                        lockCheckAndProlongInterval = Duration.ofSeconds(5)
                                ),
                                jobDisableConfig = it)
                    }
            )


}

private class JobDisableConfigEditor {
    val jobDisableConfig: AtomicProperty<JobDisableConfig> = AtomicProperty(JobDisableConfig())

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