package ru.fix.distributed.job.manager;

import ru.fix.dynamic.property.api.DynamicProperty;
import ru.fix.stdlib.concurrency.threads.Schedule;

import java.util.Optional;

/**
 * @author Ayrat Zulkarnyaev
 */
public interface DistributedJob {

    /**
     * @return id of the job.
     * To define job id you can use {@link ru.fix.distributed.job.manager.annotation.JobIdField} instead.
     * Overriding this method overrides the use of {@link ru.fix.distributed.job.manager.annotation.JobIdField}
     */
    default Optional<String> getJobId() {
        return Optional.empty();
    }

    /**
     * @return delay between job invocation
     */
    DynamicProperty<Schedule> getSchedule();

    /**
     * Method will be invoked on one of cluster machines
     */
    void run(DistributedJobContext context) throws Exception;

    /**
     * @return delay of job launching after server startup
     */
    default long getInitialJobDelay() {
        return getSchedule().get().getValue();
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
    long getWorkPoolCheckPeriod();
}
