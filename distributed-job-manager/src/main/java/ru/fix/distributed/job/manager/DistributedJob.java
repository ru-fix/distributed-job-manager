package ru.fix.distributed.job.manager;

import ru.fix.stdlib.concurrency.threads.Schedule;

/**
 * @author Ayrat Zulkarnyaev
 */
public interface DistributedJob {

    /**
     * @return id of the job.
     */
    String getJobId();

    /**
     * @return delay between job invocation
     */
    Schedule getSchedule();

    /**
     * Method will be invoked on one of cluster machines
     */
    void run(DistributedJobContext context) throws Exception;

    /**
     * @return delay of job launching after server startup
     */
    default long getInitialJobDelay() {
        return getSchedule().getValue();
    }

    /**
     * See {@link ru.fix.distributed.job.manager.util.WorkPoolUtils#checkWorkPoolItemsRestrictions}
     * for restrictions on WorkPool items
     * Возвращает пулл обрабатываемых сейчас элементов, т.е. не только элементов которые необходимо обработать, но и тех, что сейчас в процессе обработки.
     * Если элемент был в пуле, но сейчас его не передаем туда, то джоба обрабатывающая его, будет остановлена.
     */
    WorkPool getWorkPool();

    /**
     * Определеяет возможность запуска каждой
     */
    WorkPoolRunningStrategy getWorkPoolRunningStrategy();

    /**
     * Specifies period in which work pool expires and should be checked
     * with additional call on {@link #getWorkPool()} to get latest updates.
     *
     * @return period of time in milliseconds, 0 means that work pool never expires and there is no need to check
     */
    default long getWorkPoolCheckPeriod() {
        return 0;
    }
}
