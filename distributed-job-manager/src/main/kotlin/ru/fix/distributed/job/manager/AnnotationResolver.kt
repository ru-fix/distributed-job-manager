package ru.fix.distributed.job.manager

import ru.fix.distributed.job.manager.annotation.JobIdField

object AnnotationResolver {

    fun resolveJobId(job: DistributedJob) : String {
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

}