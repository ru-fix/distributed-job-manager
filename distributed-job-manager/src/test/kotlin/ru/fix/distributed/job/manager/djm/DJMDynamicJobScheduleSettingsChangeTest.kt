package ru.fix.distributed.job.manager.djm

import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import ru.fix.distributed.job.manager.*
import ru.fix.dynamic.property.api.AtomicProperty
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

internal class DJMDynamicJobScheduleSettingsChangeTest : DJMTestSuite() {

    @Test
    fun `delayed Job should start immediately if implicit initial delay changes from big to small`() {
        // initial setting - 1h delay, and implicit 1h initial delay
        val jobWithBigDelayAndImplicitBigInitialDelay = object: DistributedJob {
            val launched = AtomicBoolean()
            val schedule = AtomicProperty<Schedule>(Schedule.withDelay(TimeUnit.HOURS.toMillis(1)))

            override val jobId = JobId("jobWithBigDelayAndImplicitBigInitialDelay")
            override fun getSchedule(): DynamicProperty<Schedule> = schedule
            override fun run(context: DistributedJobContext) { launched.set(true) }
            override fun getWorkPool() = WorkPool.singleton()
            override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
            override fun getWorkPoolCheckPeriod(): Long = 0
        }

        val djm = createDJM(jobWithBigDelayAndImplicitBigInitialDelay)

        // initial 1h delay continues still, job not started
        await().during(3, TimeUnit.SECONDS).until {
            jobWithBigDelayAndImplicitBigInitialDelay.launched.get() == false
        }

        // change schedule delay setting of the job with implicit start delay settings,
        // so the job should start in moments
        jobWithBigDelayAndImplicitBigInitialDelay.schedule.set(Schedule.withDelay(TimeUnit.SECONDS.toMillis(1L)))

        await().atMost(10, TimeUnit.SECONDS).until {
            jobWithBigDelayAndImplicitBigInitialDelay.launched.get() == true
        }
    }

    @Test
    fun `delayed Job should start immediately if explicit initial delay changes from big to small`() {
        // initial setting - 1h delay, and explicit 1h initial delay
        val jobWithExplicitBigInitialDelay = object: DistributedJob {
            val launched = AtomicBoolean()
            val schedule = AtomicProperty<Schedule>(Schedule.withDelay(TimeUnit.HOURS.toMillis(1)))
            val initialDelay = AtomicProperty<Long>(TimeUnit.HOURS.toMillis(1))

            override val jobId = JobId("jobWithExplicitBigInitialDelay")
            override fun getSchedule(): DynamicProperty<Schedule> = schedule
            override fun getInitialJobDelay(): DynamicProperty<Long> = initialDelay
            override fun run(context: DistributedJobContext) { launched.set(true) }
            override fun getWorkPool() = WorkPool.singleton()
            override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
            override fun getWorkPoolCheckPeriod(): Long = 0
        }

        val djm = createDJM(jobWithExplicitBigInitialDelay)

        // initial 1h delay continues still, job not started
        await().during(3, TimeUnit.SECONDS).until {
            jobWithExplicitBigInitialDelay.launched.get() == false
        }

        // change start delay setting, so the job should start immediately
        jobWithExplicitBigInitialDelay.initialDelay.set(0)

        await().atMost(10, TimeUnit.SECONDS).until {
            jobWithExplicitBigInitialDelay.launched.get() == true
        }
    }
}