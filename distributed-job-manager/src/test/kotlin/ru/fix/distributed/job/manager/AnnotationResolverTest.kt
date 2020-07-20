package ru.fix.distributed.job.manager

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import ru.fix.distributed.job.manager.annotation.JobIdField
import ru.fix.distributed.job.manager.model.JobDescriptor
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule

internal class AnnotationResolverTest {

    @Test
    @Disabled
    fun `JobDescriptor WHEN jobId notSpecified THEN getJobId returns full class name` () {
        val descriptor = JobDescriptor(JobDoesNotDescribingJobId())
        assertEquals(
                JobDoesNotDescribingJobId::class.java.name
                        .replace('.', '-')
                        .replace('$', '_'),
                descriptor.getJobId())
    }

    @Test
    fun `JobDescriptor WHEN jobId specified by annotation THEN getJobId returns value from annotated field` () {
        val job = JobDescribingIdByAnnotation()
        val descriptor = JobDescriptor(job)
        assertEquals(job.id, descriptor.getJobId())
    }

    @Test
    fun `JobDescriptor WHEN jobId specified by method THEN getJobId delegated to that method` () {
        val job = JobDescribingIdByMethod()
        val descriptor = JobDescriptor(job)
        assertEquals(job.jobId, descriptor.getJobId())
    }

    class JobDescribingIdByAnnotation : NoopJob() {

        @JobIdField
        val id = JobId("JobDescribingIdByAnnotation")
    }

    class JobDescribingIdByMethod : NoopJob() {

        override val jobId = JobId("JobDescribingIdByMethod")
    }

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
