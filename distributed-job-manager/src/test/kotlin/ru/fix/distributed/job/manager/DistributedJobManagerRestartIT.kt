package ru.fix.distributed.job.manager

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import ru.fix.aggregating.profiler.NoopProfiler
import ru.fix.distributed.job.manager.model.DistributedJobManagerSettings
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.NamedExecutors
import ru.fix.stdlib.concurrency.threads.ReschedulableScheduler
import ru.fix.stdlib.concurrency.threads.Schedule
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue

class DistributedJobManagerRestartIT : AbstractJobManagerTest() {
    internal class DjmDescriptor(var distributedJobManager: DistributedJobManager, var number: Int)

    @Test
    @Throws(Exception::class)
    fun restartTest() {
        val jobs = createHearthBeatJobs()
        val djms = createDjms(jobs)
        val restarter = startRestarter(djms, jobs)

        try {
            checkHearthBeatsOfJobs(jobs, TEST_RESTARTS_DURATION_MS)

            restarter.shutdown()
            //после многократных рестартов (на случай, если джобы очухивались только благодаря рестартам)
            checkHearthBeatsOfJobs(jobs, TEST_AFTER_RESTARTS_DURATION_MS)
        } finally {
            restarter.shutdown()
            while (!djms.isEmpty()) {
                val descriptor = djms.remove()
                descriptor.distributedJobManager.close()
            }
        }

    }

    private fun createHearthBeatJobs(): List<HearthBeatJob> {
        val jobs: MutableList<HearthBeatJob> = ArrayList(JOB_COUNT)
        for (jobNumber in 1..JOB_COUNT) {
            jobs.add(HearthBeatJob("job-$jobNumber", JOB_SCHEDULE, WORK_ITEM_COUNT))
        }
        return jobs
    }

    @Throws(Exception::class)
    private fun createDjms(
            jobs: List<DistributedJob>
    ): BlockingQueue<DjmDescriptor> {
        val djms: BlockingQueue<DjmDescriptor> = ArrayBlockingQueue(SERVER_COUNT)
        for (serverNumber in 1..SERVER_COUNT) {
            djms.add(DjmDescriptor(createDjm(jobs, serverNumber), serverNumber))
        }
        return djms
    }

    @Throws(Exception::class)
    private fun createDjm(
            jobs: List<DistributedJob>,
            serverId: Int
    ): DistributedJobManager {
        return DistributedJobManager(
                zkTestingServer.createClient(),
                jobs,
                NoopProfiler(),
                DistributedJobManagerSettings(
                        nodeId = "worker-$serverId",
                        rootPath = JOB_MANAGER_ZK_ROOT_PATH,
                        timeToWaitTermination = DynamicProperty.of(TIMEOUT_TO_AWAIT_TERMINATION_MS)
                )
        )
    }

    private fun startRestarter(
            djms: BlockingQueue<DjmDescriptor>,
            jobs: List<HearthBeatJob>
    ): ReschedulableScheduler {
        val restartScheduler = NamedExecutors.newSingleThreadScheduler("restart-thread", NoopProfiler())
        restartScheduler.schedule(DynamicProperty.of(RESTART_SCHEDULE)) {
            restartDjm(djms, jobs)
        }
        return restartScheduler
    }

    private fun restartDjm(
            djms: BlockingQueue<DjmDescriptor>,
            jobs: List<DistributedJob>
    ) {
        val descriptor = djms.remove()
        log.info("restarting DJM-{}", descriptor.number)
        descriptor.distributedJobManager.close()
        Thread.sleep(TIME_BETWEEN_STOP_AND_START_DJM_MS)
        djms.add(
                DjmDescriptor(
                        createDjm(jobs, descriptor.number), descriptor.number
                )
        )
    }

    private fun checkHearthBeatsOfJobs(jobs: List<HearthBeatJob>, durationMs: Long) {
        val timeStart = System.currentTimeMillis()
        while(System.currentTimeMillis() - timeStart < durationMs) {
            Thread.sleep(CHECK_HEARTH_BEAT_PERIOD.toLong())
            jobs.forEach {
                checkAndResetHearthBeat(it)
            }
        }
    }

    private fun checkAndResetHearthBeat(job: HearthBeatJob) {
        for ((key, value) in job.hearthBeatMap) {
            val beatCount = value.getAndSet(0)
            assertTrue(
                    beatCount >= EXPECTED_HEARTH_BEAT_COUNT,
                    "hearth-beat is lost. workItem $key,beatCount = $beatCount, all beats: ${job.hearthBeatMap}"
            )
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(DistributedJobManagerRestartIT::class.java)
        private const val SERVER_COUNT = 6
        private const val JOB_COUNT = 10
        private const val WORK_ITEM_COUNT = 15
        private val JOB_SCHEDULE = Schedule(Schedule.Type.RATE, 500)
        private const val TIMEOUT_TO_AWAIT_TERMINATION_MS: Long = 180_000

        /**
         * регулярный перезапуск DJM-ов по очереди
         */
        private val RESTART_SCHEDULE = Schedule(Schedule.Type.DELAY, 10)
        /**
         * сколько держать очередной DJM выключенным
         * */
        private const val TIME_BETWEEN_STOP_AND_START_DJM_MS: Long = 1000
        /**
         * как часто проверять что работа идет на всех ворк-айтемах
         * */
        private const val CHECK_HEARTH_BEAT_PERIOD = 2_000
        /**
         * сколько минимум нужно "харт-битов" чтобы посчитать
         * проверку ворк-айтема успешной
         * */
        private const val EXPECTED_HEARTH_BEAT_COUNT = 1
        /**
         * Сколько времени проверять "харт-биты" при последовательных перезапусках
         * */
        private const val TEST_RESTARTS_DURATION_MS: Long = 7_000
        /**
         * Сколько времени проверять "харт-биты" после перезапусков
         * */
        private const val TEST_AFTER_RESTARTS_DURATION_MS: Long = 4_500
    }
}