package ru.fix.distributed.job.manager.model

import ru.fix.distributed.job.manager.*
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule

class JobDescriptor(private val job: DistributedJob) {

    private val jobId: String = job.getJobId() ?: AnnotationResolver.resolveJobId(job)

    fun getJobId(): String = jobId

    fun run(context: DistributedJobContext) = job.run(context)

    fun getInitialJobDelay(): DynamicProperty<Long> = job.getInitialJobDelay()

    fun getWorkPoolRunningStrategy(): WorkPoolRunningStrategy = job.getWorkPoolRunningStrategy()

    fun getSchedule(): DynamicProperty<Schedule> = job.getSchedule()

    fun getWorkPool(): WorkPool = job.getWorkPool()

    fun getWorkPoolCheckPeriod(): Long = job.getWorkPoolCheckPeriod()
}