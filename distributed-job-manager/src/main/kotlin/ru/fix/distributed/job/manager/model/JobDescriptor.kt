package ru.fix.distributed.job.manager.model

import ru.fix.distributed.job.manager.*
import ru.fix.distributed.job.manager.annotation.JobIdField
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule

/**
 * Internal DJM representation of user-defined [DistributedJob]
 * */
class JobDescriptor(private val job: DistributedJob) {

    private val jobId: JobId = resolveJobId(job)

    fun getJobId(): JobId = jobId

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
    for (field in job.javaClass.declaredFields) {
        if (field.isAnnotationPresent(JobIdField::class.java)) {
            try {
                field.isAccessible = true
                return field[job] as JobId
            } catch (e: Exception) {
                throw IllegalStateException(
                        "Some troubles with getting jobId by annotation. Maybe it's not a JobId class?", e)
            }
        }
    }
    return JobId(job.javaClass.name.replace('.', '-').replace('$', '_'))
}