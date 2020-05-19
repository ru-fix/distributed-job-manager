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
import ru.fix.dynamic.property.api.DynamicProperty
import java.time.Duration


internal class JobManagerSettingsIT : AbstractJobManagerTest() {

    private val defaultJobRunTimeoutMs = 1_000L

    @Test
    fun `WHEN disableAllJobsProperty changed THEN jobs running accordingly`() {
        val job1 = spy(defaultJob(1))
        val job2 = spy(defaultJob(2))
        val disableAllJobsProperty = AtomicProperty(true)
        createDjm(
                disableAllJobsProperty = disableAllJobsProperty,
                jobs = listOf(job1, job2)
        ).use {
            await().pollDelay(Duration.ofMillis(defaultJobRunTimeoutMs)).untilAsserted {
                verify(job1, never()).run(any())
                verify(job2, never()).run(any())
            }
            disableAllJobsProperty.set(false)
            await().atMost(Duration.ofMillis(defaultJobRunTimeoutMs)).untilAsserted {
                verify(job1, times(1)).run(any())
                verify(job2, times(1)).run(any())
            }
            disableAllJobsProperty.set(true)
            await().pollDelay(Duration.ofMillis(defaultJobRunTimeoutMs)).untilAsserted {
                verify(job1, times(1)).run(any())
                verify(job2, times(1)).run(any())
            }
            disableAllJobsProperty.set(false)
            await().atMost(Duration.ofMillis(defaultJobRunTimeoutMs)).untilAsserted {
                verify(job1, times(2)).run(any())
                verify(job2, times(2)).run(any())
            }
        }
    }

    private fun defaultJob(
            jobId: Int = 1,
            workItems: Set<String> = setOf("1", "2"),
            delay: Long = 100,
            workPoolCheckPeriod: Long = 0
    ): StubbedMultiJob = StubbedMultiJob(
            jobId, workItems, delay, workPoolCheckPeriod
    )

    private fun createDjm(
            nodeId: String = "1",
            rootPath: String = "DistributedJobConfigIT",
            timeToWaitTermination: DynamicProperty<Long> = DynamicProperty.of(180_000),
            assignmentStrategy: AssignmentStrategy = AssignmentStrategies.DEFAULT,
            disableAllJobsProperty: DynamicProperty<Boolean> = DynamicProperty.of(false),
            jobs: Collection<DistributedJob>,
            curatorFramework: CuratorFramework = zkTestingServer.createClient(),
            profiler: Profiler = NoopProfiler()
    ) = DistributedJobManager(
            curatorFramework,
            jobs,
            profiler,
            DistributedJobManagerSettings(
                    nodeId, rootPath, assignmentStrategy, timeToWaitTermination, disableAllJobsProperty
            )
    )

}