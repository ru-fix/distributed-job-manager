package ru.fix.distributed.job.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.stdlib.concurrency.threads.Schedule;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public abstract class SingletonJobAbstract implements DistributedJob {

    private static final Logger log = LoggerFactory.getLogger(SingletonJobAbstract.class);


    protected static final String JOB_DELAY_POSTFIX_PROPERTY = ".period.seconds";
    protected static final String JOB_DELAY_DEFAULT_VALUE_PROPERTY = "3600"; //1 час
    protected static final String JOB_DELAY_DESCRIPTION_PROPERTY = "Периодичность запуска задачи. Время в секундах.";

    protected static final String DISABLE_FLAG_POSTFIX_PROPERTY = ".disable";
    protected static final String DISABLE_FLAG_DEFAULT_VALUE_PROPERTY = "true";
    protected static final String DISABLE_FLAG_DESCRIPTION_PROPERTY = "Признак выключенности задачи. " +
            "true - выключена, false - включена";


    @Override
    public WorkPool getWorkPool() {
        return WorkPool.of(Collections.singleton("all"));
    }


    @Override
    public WorkPoolRunningStrategy getWorkPoolRunningStrategy() {
        return WorkPoolRunningStrategies.getSingleThreadStrategy();
    }

    @Override
    public Schedule getSchedule() {
        return Schedule.withDelay(TimeUnit.SECONDS.toMillis(getJobDelaySec()));
    }

    /**
     * Состояние задачи (выключена/включена)
     * <p>
     * флаг в zk - "${jobId}.job.disable"
     *
     * @return - true - выключена, false - включена
     */
    protected abstract Boolean getDisableFlag();

    /**
     * Периодичность запуска задачи
     * <p>
     * флаг в zk - "${jobId}.job.delay.sec"
     *
     * @return - время в секундах
     */
    protected abstract Long getJobDelaySec();


    @Override
    public void run(DistributedJobContext context) {
        if (getDisableFlag()) {
            return;
        }
        Set<String> workShare = context.getWorkShare();
        for (String workShareName : workShare) {
            if (context.isNeedToShutdown() || getDisableFlag()) {
                return;
            }
            try {
                log.debug("{} started, lockKeyId '{}'", getJobId(), workShareName);
                runJob(workShareName, () -> !context.isNeedToShutdown(), context::addShutdownListener);
                log.debug("{} finished, lockKeyId '{}'", getJobId(), workShareName);
            } catch (Exception ex) {
                log.error("Error to run " + getJobId(), ex);
            }
        }
    }


    /**
     * Запуск работы джобы для конкретной задачи
     *
     * @param workShareName       - задача которую нужно выполнит
     *                            по умолчанию приходит значение "all",
     *                            если есть WorkPool для выполнение в задачи, то нужно переопределить метод
     *                            getWorkPool()
     * @param jobStatus           - интерфейс для проверки экстренного завершение задачи
     * @param addShutdownListener - консьюмер, позволяющий добавить в ShutdownListener
     */
    protected abstract void runJob(String workShareName,
                                   JobStatus jobStatus,
                                   Consumer<ShutdownListener> addShutdownListener);


    public interface JobStatus {
        /**
         * Продлевает установленный лок на задачу и проверяет:
         * 1. проверка состояние задачи, если состояние "выключена" то вернётся false
         * 2. проверяет индикатор немедленной остановки задачи, если нужно прекратить выполнение то вернётся false
         * 3. проверяет срок действия установленной блокировки, если блокировка просрочена то вернётся false
         *
         * @return true - можно продолжать выполнять задачу, false - нужно завершать задачу
         */
        boolean canContinueJob();
    }

}
