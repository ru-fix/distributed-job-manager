package ru.fix.distributed.job.manager.djm

import org.apache.curator.framework.CuratorFramework
import org.awaitility.Awaitility.await
import org.hamcrest.MatcherAssert
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
import java.util.concurrent.TimeUnit

class DJMManagerCleaningTest : DJMTestSuite() {

    @Test
    fun `WHEN last djm with available job closed THEN removing job from workPool`() {
        class JobForCleaning(id: String) : DistributedJob {
            override val jobId = JobId("JobForCleaning-$id")
            override fun getSchedule() = DynamicProperty.of(Schedule.withDelay(100))
            override fun run(context: DistributedJobContext) {}
            override fun getWorkPool() = WorkPool.singleton()
            override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
            override fun getWorkPoolCheckPeriod(): Long = 0
        }
        val job1 = JobForCleaning("1")
        val job2 = JobForCleaning("2")
        val job3 = JobForCleaning("3")

        val workPoolCleanPeriod = AtomicProperty(500L)
        val cleaningPerformTimeoutMs: Long = 1000
        val closingDjmTimeoutMs: Long = 1500

        val jobManager1 = createDJM(
                jobs = listOf(job1, job2),
                workPoolCleanPeriod = workPoolCleanPeriod
        )
        val jobManager2 = createDJM(
                jobs = listOf(job2, job3),
                workPoolCleanPeriod = workPoolCleanPeriod
        )

        await().atMost(1, TimeUnit.MINUTES).until {
            server.client.children
                    .forPath(djmZkPathsManager.availableWorkPool())
                    .contains(resolveJobId(job1).id)
        }
        closeDjm(jobManager1)

        awaitCleaningJob(
                atLeast = 0,
                atMost = workPoolCleanPeriod.get() + cleaningPerformTimeoutMs,
                jobIdForRemoval = resolveJobId(job1).id, curator = server.client)

        val jobManager3 = createDJM(
                jobs = listOf(job1, job2),
                workPoolCleanPeriod = workPoolCleanPeriod
        )
        await().atMost(1, TimeUnit.MINUTES).until {
            server.client.children
                    .forPath(djmZkPathsManager.availableWorkPool())
                    .contains(resolveJobId(job1).id)
        }

        val oldCleanPeriodMs = workPoolCleanPeriod.set(7000L)
        await().pollDelay(Duration.ofMillis(oldCleanPeriodMs)).untilAsserted {}

        closeDjm(jobManager2)

        awaitCleaningJob(
                atLeast = workPoolCleanPeriod.get() - closingDjmTimeoutMs - oldCleanPeriodMs,
                atMost = workPoolCleanPeriod.get() + cleaningPerformTimeoutMs,
                jobIdForRemoval = resolveJobId(job3).id, curator = server.client)

        closeDjm(jobManager3)
    }

    private fun awaitCleaningJob(
            atLeast: Long,
            atMost: Long,
            jobIdForRemoval: String,
            curator: CuratorFramework) {
        await()
                .atLeast(atLeast, TimeUnit.MILLISECONDS)
                .atMost(atMost, TimeUnit.MILLISECONDS)
                .untilAsserted {
                    val jobsFromZkWorkPool = curator.children
                            .forPath(djmZkPathsManager.availableWorkPool())

                    MatcherAssert.assertThat(
                            "cleaning wasn't performed during period." + printDjmZkTree(),
                            !jobsFromZkWorkPool.contains(jobIdForRemoval)
                    )
                }
    }


    fun createDJM(jobs: List<DistributedJob>, workPoolCleanPeriod: DynamicProperty<Long>) =
            createDJM(
                    jobs = jobs,
                    settings = workPoolCleanPeriod.map {
                        DistributedJobManagerSettings(
                                timeToWaitTermination = 10000,
                                workPoolCleanPeriod = it,
                                lockManagerConfig = PersistentExpiringLockManagerConfig(
                                        lockAcquirePeriod = Duration.ofSeconds(15),
                                        expirationPeriod = Duration.ofSeconds(5),
                                        lockCheckAndProlongInterval = Duration.ofSeconds(5)
                                )
                        )
                    }
            )

}