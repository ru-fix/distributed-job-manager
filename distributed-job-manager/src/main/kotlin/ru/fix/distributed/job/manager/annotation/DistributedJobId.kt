package ru.fix.distributed.job.manager.annotation

/**
 * For String field in DistributedJobs, which represents id of that job
 * If field with that annotation won't be found, for job id will be used full class name
 *
 * Now you can change that behaviour by overriding {@link ru.fix.distributed.job.manager.DistributedJob}
 * */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class DistributedJobId(
        val value: String
)