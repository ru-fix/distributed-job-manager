package ru.fix.distributed.job.manager.model

import ru.fix.distributed.job.manager.*
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule

class JobDescriptor(private val job: DistributedJob) {

    private val jobId: String = job.jobId
            .orElse(AnnotationResolver.resolveJobId(job))

    fun getJobId(): String = jobId

    fun run(context: DistributedJobContext?) = job.run(context)

    fun getInitialJobDelay(): Long = job.initialJobDelay

    fun getWorkPoolRunningStrategy(): WorkPoolRunningStrategy = job.workPoolRunningStrategy

    fun getSchedule(): DynamicProperty<Schedule> = job.schedule

    fun getWorkPool(): WorkPool = job.workPool

    fun getWorkPoolCheckPeriod(): Long = job.workPoolCheckPeriod
}