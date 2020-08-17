package ru.fix.distributed.job.manager.model

import ru.fix.distributed.job.manager.*
import ru.fix.distributed.job.manager.annotation.DistributedJobId
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule

/**
 * Internal DJM representation of user-defined [DistributedJob]
 * */
class JobDescriptor(private val job: DistributedJob) {

    val jobId: JobId = resolveJobId(job)

    fun run(context: DistributedJobContext) = job.run(context)

    fun getInitialJobDelay(): DynamicProperty<Long> = job.getInitialJobDelay()

    fun getWorkPoolRunningStrategy(): WorkPoolRunningStrategy = job.getWorkPoolRunningStrategy()

    fun getSchedule(): DynamicProperty<Schedule> = job.getSchedule()

    fun getWorkPool(): WorkPool = job.getWorkPool()

    fun getWorkPoolCheckPeriod(): Long = job.getWorkPoolCheckPeriod()
}

fun resolveJobId(job: DistributedJob): JobId {
    val jobIdByGetter = job.jobId
    if (jobIdByGetter != null) {
        return jobIdByGetter
    }
    val jobIdAnnotation = job.javaClass.getAnnotation(DistributedJobId::class.java)
    if (jobIdAnnotation != null) {
        return JobId(jobIdAnnotation.value)
    }
    return JobId(job.javaClass.name.replace('$', '_'))
}