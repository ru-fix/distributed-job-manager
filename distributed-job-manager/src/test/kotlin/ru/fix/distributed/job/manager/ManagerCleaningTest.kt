package ru.fix.distributed.job.manager

import org.apache.curator.framework.CuratorFramework
import org.awaitility.Awaitility.await
import org.hamcrest.MatcherAssert
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import ru.fix.dynamic.property.api.AtomicProperty
import java.time.Duration
import java.util.concurrent.TimeUnit

internal class ManagerCleaningTest : AbstractJobManagerTest() {

    @Test
    @Throws(Exception::class)
    fun `WHEN last djm with available job closed THEN removing job from workPool`() {
        val workPoolCleanPeriod = AtomicProperty(500L)
        val cleaningPerformTimeoutMs: Long = 1000
        val closingDjmTimeoutMs: Long = 1500
        val job1: DistributedJob = StubbedMultiJob(1, setOf("all"))
        val job2: DistributedJob = StubbedMultiJob(2, setOf("all"))
        val job3: DistributedJob = StubbedMultiJob(3, setOf("all"))
        val curator1 = defaultZkClient()
        val jobManager1 = createNewJobManager(
                curatorFramework = curator1,
                jobs = listOf(job1, job2),
                workPoolCleanPeriod = workPoolCleanPeriod
        )
        val curator2 = defaultZkClient()
        val jobManager2 = createNewJobManager(
                curatorFramework = curator2,
                jobs = listOf(job2, job3),
                workPoolCleanPeriod = workPoolCleanPeriod
        )
        assertTrue(curator2.children.forPath(paths.availableWorkPool()).contains(job1.jobId.id))
        jobManager1.close()
        curator1.close()
        awaitCleaningJob(
                atLeast = 0,
                atMost = workPoolCleanPeriod.get() + cleaningPerformTimeoutMs,
                jobIdForRemoval = job1.jobId.id, curator = curator2)
        val curator3 = defaultZkClient()
        val jobManager3 = createNewJobManager(
                curatorFramework = curator3,
                jobs = listOf(job1, job2),
                workPoolCleanPeriod = workPoolCleanPeriod
        )
        assertTrue(curator3.children.forPath(paths.availableWorkPool()).contains(job1.jobId.id))
        val oldCleanPeriodMs = workPoolCleanPeriod.set(7000L)
        await().pollDelay(Duration.ofMillis(oldCleanPeriodMs)).untilAsserted {}
        jobManager2.close()
        curator2.close()
        awaitCleaningJob(
                atLeast = workPoolCleanPeriod.get() - closingDjmTimeoutMs - oldCleanPeriodMs,
                atMost = workPoolCleanPeriod.get() + cleaningPerformTimeoutMs,
                jobIdForRemoval = job3.jobId.id, curator = curator3)
        jobManager3.close()
        curator3.close()
    }

    private fun awaitCleaningJob(atLeast: Long, atMost: Long, jobIdForRemoval: String, curator: CuratorFramework) {
        await()
                .atLeast(atLeast, TimeUnit.MILLISECONDS)
                .atMost(atMost, TimeUnit.MILLISECONDS)
                .untilAsserted {
                    val jobsFromZkWorkPool = curator.children.forPath(paths.availableWorkPool())
                    MatcherAssert.assertThat(
                            "cleaning wasn't performed during period." + printDjmZkTree(),
                            !jobsFromZkWorkPool.contains(jobIdForRemoval)
                    )
                }
    }

}