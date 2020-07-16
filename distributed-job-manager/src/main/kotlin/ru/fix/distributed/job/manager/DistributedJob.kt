package ru.fix.distributed.job.manager

import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule

interface DistributedJob {
    /**
     * @return id of the job.
     * To define job id you can use {@link ru.fix.distributed.job.manager.annotation.JobIdField} instead.
     * Overriding this method overrides the use of {@link ru.fix.distributed.job.manager.annotation.JobIdField}
     */
    fun getJobId(): String? = null

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
     * See [ru.fix.distributed.job.manager.util.WorkPoolUtils.checkWorkPoolItemsRestrictions]
     * for restrictions on WorkPool items
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