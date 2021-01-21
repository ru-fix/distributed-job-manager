package ru.fix.distributed.job.manager.djm

import io.kotest.assertions.throwables.shouldThrowExactly
import io.kotest.matchers.shouldBe
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import ru.fix.distributed.job.manager.*
import ru.fix.distributed.job.manager.flight.control.JobStateInspector
import ru.fix.distributed.job.manager.flight.control.JobStateInspectorImpl
import ru.fix.distributed.job.manager.flight.control.JobStatus
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule
import java.time.Duration
import java.util.concurrent.TimeUnit

class DJMJobStateInspectorTest : DJMTestSuite() {

    companion object {
        const val JOB_ID = "flight_control_test_job"
        const val DELAY_SECONDS = 5L
    }

    lateinit var jobStateInspector: JobStateInspector
    @BeforeEach
    fun init() {
        jobStateInspector = JobStateInspectorImpl(
            zkRootPaths = setOf(this.djmZkRootPath),
            curatorFramework = this.server.client
        )
    }

    @Test
    fun `when job is running then return RUN status`() {
        val job = FlightControlTestJob()
        createDJM(job)

        await().atMost(Duration.ofSeconds(DELAY_SECONDS * 2)).untilAsserted {
            job.status shouldBe JobStatus.STARTED
            jobStateInspector.getJobStatus(JOB_ID) shouldBe JobStatus.STARTED
        }

        await().atMost(Duration.ofSeconds(DELAY_SECONDS * 2)).untilAsserted {
            job.status shouldBe JobStatus.STOPPED
            jobStateInspector.getJobStatus(JOB_ID) shouldBe JobStatus.STOPPED
        }
    }

    @Test
    fun `when JobId is not available then throw Exception`() {
        createDJM(FlightControlTestJob("test_id"))

        shouldThrowExactly<IllegalArgumentException> {
            jobStateInspector.getJobStatus(JOB_ID)
        }
    }

    class FlightControlTestJob(jobId: String = JOB_ID) : DistributedJob {
        var status = JobStatus.STOPPED
        override val jobId = JobId(jobId)

        override fun getSchedule() = DynamicProperty.of(Schedule.withRate(Duration.ofSeconds(DELAY_SECONDS).toMillis()))

        override fun run(context: DistributedJobContext) {
            status = JobStatus.STARTED
            TimeUnit.SECONDS.sleep(3)
            status = JobStatus.STOPPED
        }

        override fun getWorkPool() = WorkPool.singleton()
        override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
        override fun getWorkPoolCheckPeriod() = 0L
    }
}
