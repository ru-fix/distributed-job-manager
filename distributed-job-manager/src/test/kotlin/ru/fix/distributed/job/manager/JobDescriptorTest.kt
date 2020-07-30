package ru.fix.distributed.job.manager

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import ru.fix.distributed.job.manager.annotation.DistributedJobId
import ru.fix.distributed.job.manager.model.JobDescriptor
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule

internal class JobDescriptorTest {

    @Test
    fun `JobDescriptor WHEN jobId notSpecified THEN jobId is full class name with dots replaced by '-' and '$' replaced by '_'`() {
        val descriptor = JobDescriptor(JobDoesNotDescribingJobId())
        assertEquals(
                JobDoesNotDescribingJobId::class.java.name
                        .replace('.', '-')
                        .replace('$', '_'),
                descriptor.jobId.id)
    }

    @Test
    fun `JobDescriptor WHEN jobId specified by annotation THEN jobId is value from annotated field`() {
        val job = JobDescribingIdByAnnotation()
        val descriptor = JobDescriptor(job)
        assertEquals(job.javaClass.getAnnotation(DistributedJobId::class.java).value, descriptor.jobId.id)
    }

    @Test
    fun `JobDescriptor WHEN jobId specified by val THEN jobId is that val`() {
        val job = JobDescribingIdByVal()
        val descriptor = JobDescriptor(job)
        assertEquals(job.jobId, descriptor.jobId)
    }

    class JobDescribingIdByVal : NoopJob() {
        override val jobId = JobId("JobDescribingIdByMethod")
    }

    @DistributedJobId("JobDescribingIdByAnnotation")
    class JobDescribingIdByAnnotation : NoopJob()

    class JobDoesNotDescribingJobId : NoopJob()

    open class NoopJob : DistributedJob {
        override fun getSchedule(): DynamicProperty<Schedule> {
            throw UnsupportedOperationException()
        }

        override fun run(context: DistributedJobContext) {
            throw UnsupportedOperationException()
        }

        override fun getWorkPool(): WorkPool {
            throw UnsupportedOperationException()
        }

        override fun getWorkPoolRunningStrategy(): WorkPoolRunningStrategy {
            throw UnsupportedOperationException()
        }

        override fun getWorkPoolCheckPeriod(): Long {
            throw UnsupportedOperationException()
        }
    }

}
