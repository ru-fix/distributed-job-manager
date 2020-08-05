package ru.fix.distributed.job.manager.annotation

import ru.fix.distributed.job.manager.DistributedJob

/**
 * For declaring identifier of job by annotation:
 * ```
 * @DistributedJobId("some-job")
 * class SomeJob : DistributedJob {
 * //...
 * }
 * ```
 *
 * This annotation will be ignored, if you override [DistributedJob.jobId].
 *
 * If jobId is not defined, full class name of job will be used as jobId.
 * @see DistributedJob.jobId
 * */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class DistributedJobId(
        val value: String
)