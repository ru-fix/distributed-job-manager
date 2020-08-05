package ru.fix.distributed.job.manager

import io.kotest.matchers.longs.shouldBeGreaterThanOrEqual
import io.kotest.matchers.shouldBe
import org.awaitility.Awaitility
import org.junit.jupiter.api.Test
import ru.fix.aggregating.profiler.AggregatingProfiler
import ru.fix.aggregating.profiler.ProfiledCallReport
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule
import java.lang.Thread.sleep
import java.util.concurrent.TimeUnit

class DistributedJobManagerProfilingTest : DjmTestSuite() {

    @Test
    fun `djm profiles how many time it took to start and to close DJM instance`() {
        val job = object : DistributedJob {
            override val jobId = JobId("job")
            override fun getSchedule() = DynamicProperty.of(Schedule.withDelay(10))
            override fun run(context: DistributedJobContext) {}
            override fun getWorkPool() = WorkPool.singleton()
            override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
            override fun getWorkPoolCheckPeriod() = 0L
        }

        val profiler = AggregatingProfiler()
        val reporter = profiler.createReporter()
        val djm = createDJM(job, profiler)

        val initReport = reporter.buildReportAndReset()
        initReport.profilerCallReports
                .single { it.identity.name == ProfilerMetrics.DJM_INIT }
                .apply {
                    stopSum.shouldBe(1)
                    latencyAvg.shouldBeGreaterThanOrEqual(0)
                }

        djm.close()

        val closeReport = reporter.buildReportAndReset()
        closeReport.profilerCallReports
                .single { it.identity.name == ProfilerMetrics.DJM_CLOSE }
                .apply {
                    stopSum.shouldBe(1)
                    latencyAvg.shouldBeGreaterThanOrEqual(0)
                }

    }

    @Test
    fun `each job launch is profiled`() {
        val oneSecondJob = object : DistributedJob {
            override val jobId = JobId("oneSecondJob")
            override fun getSchedule() = DynamicProperty.of(Schedule.withDelay(1000))
            override fun run(context: DistributedJobContext) {
                Thread.sleep(1000)
            }
            override fun getWorkPool() = WorkPool.singleton()
            override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
            override fun getWorkPoolCheckPeriod() = 0L
        }

        val profiler = AggregatingProfiler()
        val reporter = profiler.createReporter()
        val djm = createDJM(oneSecondJob, profiler)

        fun awaitFor(condition: (ProfiledCallReport) -> Boolean) {
            Awaitility.await().pollInterval(100, TimeUnit.MILLISECONDS).atMost(10, TimeUnit.SECONDS).until {
                reporter.buildReportAndReset().profilerCallReports
                        .singleOrNull { it.identity.name == ProfilerMetrics.JOB(JobId("oneSecondJob")) }
                        ?.let {
                            condition(it)
                        } ?: false
            }
        }

        awaitFor { it.startSum == 1L && it.stopSum == 0L && it.activeCallsCountMax == 1L }

        awaitFor { it.activeCallsCountMax == 0L }

        awaitFor { it.latencyMax >= 800L }

        djm.close()
    }

    @Test
    fun `thread pools, executing Job run and getWorkPool methods, are profiled`() {
        val oneSecondJob = object : DistributedJob {
            override val jobId = JobId("oneSecondJob")
            override fun getSchedule() = DynamicProperty.of(Schedule.withDelay(1000))
            override fun run(context: DistributedJobContext) {
                Thread.sleep(1000)
            }
            override fun getWorkPool() = WorkPool.singleton()
            override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
            override fun getWorkPoolCheckPeriod() = 0L
        }

        val profiler = AggregatingProfiler()
        val reporter = profiler.createReporter()
        val djm = createDJM(oneSecondJob, profiler)

        println("REPORT: "+ reporter.buildReportAndReset())

        Awaitility.await().pollInterval(100, TimeUnit.MILLISECONDS).atMost(10, TimeUnit.SECONDS).until {
            reporter.buildReportAndReset().profilerCallReports
                    .singleOrNull { it.identity.name == "djm.pool.job-scheduler" }
                    ?.let {
                        it.stopSum > 0
                    } ?: false
        }

        djm.close()
    }
}