package ru.fix.distributed.job.manager.djm

import io.kotest.matchers.longs.shouldBeGreaterThanOrEqual
import io.kotest.matchers.shouldBe
import org.awaitility.Awaitility
import org.junit.jupiter.api.Test
import ru.fix.aggregating.profiler.AggregatingProfiler
import ru.fix.aggregating.profiler.ProfiledCallReport
import ru.fix.distributed.job.manager.*
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

class DJMProfilingTest : DJMTestSuite() {

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
        println("Init report: $initReport")
        initReport.profilerCallReports
                .single { it.identity.name == "djm.init" }
                .apply {
                    stopSum.shouldBe(1)
                    latencyAvg.shouldBeGreaterThanOrEqual(0)
                }

        closeDjm(djm)

        val closeReport = reporter.buildReportAndReset()
        println("Close report: $initReport")
        closeReport.profilerCallReports
                .single { it.identity.name == "djm.close" }
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
                        .singleOrNull { it.identity.name == "djm." + ProfilerMetrics.JOB(JobId("oneSecondJob")) }
                        ?.let {
                            condition(it)
                        } ?: false
            }
        }

        awaitFor { it.startSum == 1L && it.stopSum == 0L && it.activeCallsCountMax == 1L }

        awaitFor { it.activeCallsCountMax == 0L }

        awaitFor { it.latencyMax >= 800L }

        closeDjm(djm)
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
            override fun getWorkPoolCheckPeriod() = 100L
        }

        val profiler = AggregatingProfiler()
        val reporter = profiler.createReporter()
        val djm = createDJM(oneSecondJob, profiler)

        class Condition
        val runJobProfiled = Condition()
        val getWorkPoolProfiled = Condition()
        val fulfilledConditions = ConcurrentHashMap.newKeySet<Condition>()

        Awaitility.await()
                .pollInterval(300, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until {

                    val report = reporter.buildReportAndReset()
                    val profiledCallsReport = report.profilerCallReports

                    if (profiledCallsReport.any {
                                it.identity.name == "djm.pool.job-scheduler.run" && it.stopSum > 0
                            }) fulfilledConditions += runJobProfiled

                    if (profiledCallsReport.any {
                                it.identity.name == "djm.pool.check-work-pool.run" && it.stopSum > 0
                            }) fulfilledConditions += getWorkPoolProfiled

                    println("REPORT: " + report)

                    fulfilledConditions.containsAll(listOf(runJobProfiled, getWorkPoolProfiled))
                }
        closeDjm(djm)
    }
}