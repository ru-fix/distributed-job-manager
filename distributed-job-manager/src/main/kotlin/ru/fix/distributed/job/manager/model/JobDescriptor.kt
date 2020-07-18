package ru.fix.distributed.job.manager.model

import ru.fix.distributed.job.manager.DistributedJob
import ru.fix.distributed.job.manager.DistributedJobContext
import ru.fix.distributed.job.manager.WorkPool
import ru.fix.distributed.job.manager.WorkPoolRunningStrategy
import ru.fix.distributed.job.manager.annotation.JobIdField
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule

/**
 * Internal DJM representation of user-defined [DistributedJob]
 * */
class JobDescriptor(private val job: DistributedJob) {

    private val jobId: String = resolveJobId(job)

    fun getJobId(): String = jobId

    fun run(context: DistributedJobContext) = job.run(context)

    fun getInitialJobDelay(): DynamicProperty<Long> = job.getInitialJobDelay()

    fun getWorkPoolRunningStrategy(): WorkPoolRunningStrategy = job.getWorkPoolRunningStrategy()

    fun getSchedule(): DynamicProperty<Schedule> = job.getSchedule()

    fun getWorkPool(): WorkPool = job.getWorkPool()

    fun getWorkPoolCheckPeriod(): Long = job.getWorkPoolCheckPeriod()
}

fun resolveJobId(job: DistributedJob): String {
    val jobIdByGetter = job.getJobId()
    if (jobIdByGetter != null) {
        return jobIdByGetter
    }
    for (field in job.javaClass.declaredFields) {
        if (field.isAnnotationPresent(JobIdField::class.java)) {
            try {
                field.isAccessible = true
                return field[job] as String
            } catch (e: Exception) {
                throw IllegalStateException(
                        "Some troubles with getting jobId by annotation. Maybe it's not a String field?", e)
            }
        }
    }
    return job.javaClass.name
}