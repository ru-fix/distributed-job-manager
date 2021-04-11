package ru.fix.distributed.job.manager.djm

import io.kotest.assertions.throwables.shouldThrowExactly
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import ru.fix.distributed.job.manager.*
import ru.fix.distributed.job.manager.flight.control.JobStateInspector
import ru.fix.distributed.job.manager.flight.control.JobStatus
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule
import java.time.Duration

class DJMJobStateInspectorTest : DJMTestSuite() {

    companion object {
        const val JOB_ID = "flight_control_test_job"
        const val DELAY_SECONDS = 5L
    }

    lateinit var jobStateInspector: JobStateInspector

    @BeforeEach
    fun init() {
        jobStateInspector = JobStateInspector(
            zkRootPath = this.djmZkRootPath,
            curatorFramework = this.server.client
        )
    }

    @Test
    fun `when job is running then return RUN status`() {
        val job = FlightControlTestJob()
        createDJM(job)

        await().atMost(Duration.ofSeconds(DELAY_SECONDS * 2)).untilAsserted {
            job.status shouldBe JobStatus.RUNNING
            jobStateInspector.getJobStatus(JOB_ID) shouldBe JobStatus.RUNNING
        }

        await().atMost(Duration.ofSeconds(DELAY_SECONDS * 2)).untilAsserted {
            job.status shouldBe JobStatus.STOPPED
            jobStateInspector.getJobStatus(JOB_ID) shouldBe JobStatus.STOPPED
        }
    }

    @Test
    fun `when JobId is not available then throw Exception`() {
        createDJM(FlightControlTestJob("job_that_exists_in_djm"))

        shouldThrowExactly<IllegalArgumentException> {
            jobStateInspector.getJobStatus("job_that_does_not_exist")
        }
    }

    class FlightControlTestJob(jobId: String = JOB_ID) : DistributedJob {
        @Volatile
        var status = JobStatus.STOPPED
        override val jobId = JobId(jobId)
        val mutex = Mutex()

        override fun getSchedule() = DynamicProperty.of(Schedule.withDelay(Duration.ofSeconds(DELAY_SECONDS).toMillis()))

        override fun run(context: DistributedJobContext) = runBlocking {
            mutex.withLock {
                status = JobStatus.RUNNING
                delay(Duration.ofSeconds(DELAY_SECONDS).toMillis())
                status = JobStatus.STOPPED
            }
        }

        override fun getWorkPool() = WorkPool.singleton()
        override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
        override fun getWorkPoolCheckPeriod() = 0L
    }
}
