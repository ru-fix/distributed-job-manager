package ru.fix.distributed.job.manager

import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule

interface DistributedJob {
    /**
     * [jobId] is unique identifier of a job type.
     * There will be several instances of a [DistributedJob] within the cluster.
     * Only one [DistributedJob] instance with same job id can be registered within single [DistributedJobManager] instance
     * There could be several [DistributedJob] instances with same job id within cluster of [DistributedJobManager]s.
     *
     * [WorkPool] returned by [getWorkPool] will be distributed among [DistributedJob] instances that returned same job id.
     * according to [ru.fix.distributed.job.manager.strategy.AssignmentStrategy]
     * provided to [DistributedJobManager]
     *
     * [DistributedJob] is not allowed to change it's job id after registration within [DistributedJobManager]
     *
     * ```
     * class FooJob: DistributedJob{
     *   fun getJobId() = "Foo"
     *   ...
     * }
     * class BarJob: DistributedJob{
     *   fun getJobId() = "Bar"
     *   ...
     * }
     * ```
     *
     * @return identifier of the job type.
     */
    val jobId: JobId

    /**
     * @return delay between job invocation
     */
    fun getSchedule(): DynamicProperty<Schedule>

    /**
     * Method will be invoked on one of cluster machines
     */
    @Throws(Exception::class)
    fun run(context: DistributedJobContext)

    /**
     * @return delay of job launching after server startup
     */
    @JvmDefault
    fun getInitialJobDelay(): DynamicProperty<Long> {
        return getSchedule().map { it.value }
    }

    /**
     * Each job has a [WorkPool]
     * [WorkPool] represent set of items to process for the cluster.
     * Since there could be several [DistributedJobManager] instances within the cluster, each of them could invoke
     * this method.
     *
     * This items will be passed to each [DistributedJob] launch through [JobContext.getWorkShare]
     * Items will be distributed among [DistributedJob] instances within the cluster
     * according to [ru.fix.distributed.job.manager.strategy.AssignmentStrategy]
     *
     * WorkPool item should be a latin string [a-zA-Z0-9_.-] no more that [WorkPool.WORK_POOL_ITEM_MAX_LENGTH] size
     *
     *
     * Возвращает пулл обрабатываемых сейчас элементов, т.е. не только элементов которые необходимо обработать, но и тех, что сейчас в процессе обработки.
     * Если элемент был в пуле, но сейчас его не передаем туда, то джоба обрабатывающая его, будет остановлена.
     */
    fun getWorkPool(): WorkPool

    /**
     * Определеяет возможность запуска каждой
     */
    fun getWorkPoolRunningStrategy(): WorkPoolRunningStrategy

    /**
     * Specifies period in which work pool expires and should be checked
     * with additional call on [.getWorkPool] to get latest updates.
     *
     * @return period of time in milliseconds, 0 means that work pool never expires and there is no need to check
     */
    fun getWorkPoolCheckPeriod(): Long
}