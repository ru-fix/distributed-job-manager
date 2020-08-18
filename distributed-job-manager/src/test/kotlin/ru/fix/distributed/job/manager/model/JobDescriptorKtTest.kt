package ru.fix.distributed.job.manager.model

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import ru.fix.distributed.job.manager.*
import ru.fix.distributed.job.manager.annotation.DistributedJobId
import ru.fix.distributed.job.manager.model.JobIdResolver.resolveJobId
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule

internal class JobDescriptorKtTest {

    @Test
    fun `if id not provided using jobId overriding, use annotation's value of job id`() {
        resolveJobId(TestJob()).id shouldBe "test.job"
    }

    @Test
    fun `if id not provided, use javaClass's name for job id`() {
        resolveJobId(TestJobWithoutJobId()).id shouldBe
                "ru.fix.distributed.job.manager.model.JobDescriptorKtTest_TestJobWithoutJobId"
    }

    @Test
    fun `annotation provided, but property also provided and use it as jobId`() {
        resolveJobId(TestJobWithId()).id shouldBe "test.job.primary"
    }

    @DistributedJobId("test.job")
    class TestJob : DistributedJob {
        override val jobId: JobId?
            get() = super.jobId

        override fun getSchedule(): DynamicProperty<Schedule> {
            TODO("Not yet implemented")
        }

        override fun run(context: DistributedJobContext) {
            TODO("Not yet implemented")
        }

        override fun getWorkPool(): WorkPool {
            TODO("Not yet implemented")
        }

        override fun getWorkPoolRunningStrategy(): WorkPoolRunningStrategy {
            TODO("Not yet implemented")
        }

        override fun getWorkPoolCheckPeriod(): Long {
            TODO("Not yet implemented")
        }
    }

    class TestJobWithoutJobId : DistributedJob {
        override val jobId: JobId?
            get() = super.jobId

        override fun getSchedule(): DynamicProperty<Schedule> {
            TODO("Not yet implemented")
        }

        override fun run(context: DistributedJobContext) {
            TODO("Not yet implemented")
        }

        override fun getWorkPool(): WorkPool {
            TODO("Not yet implemented")
        }

        override fun getWorkPoolRunningStrategy(): WorkPoolRunningStrategy {
            TODO("Not yet implemented")
        }

        override fun getWorkPoolCheckPeriod(): Long {
            TODO("Not yet implemented")
        }
    }

    @DistributedJobId("test.job")
    class TestJobWithId : DistributedJob {
        override val jobId: JobId = JobId("test.job.primary")

        override fun getSchedule(): DynamicProperty<Schedule> {
            TODO("Not yet implemented")
        }

        override fun run(context: DistributedJobContext) {
            TODO("Not yet implemented")
        }

        override fun getWorkPool(): WorkPool {
            TODO("Not yet implemented")
        }

        override fun getWorkPoolRunningStrategy(): WorkPoolRunningStrategy {
            TODO("Not yet implemented")
        }

        override fun getWorkPoolCheckPeriod(): Long {
            TODO("Not yet implemented")
        }
    }
}