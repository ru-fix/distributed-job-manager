package ru.fix.distributed.job.manager

import io.kotest.matchers.booleans.shouldBeFalse
import org.apache.logging.log4j.kotlin.Logging
import org.junit.jupiter.api.*
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule
import java.lang.Thread.sleep
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean


@TestInstance(TestInstance.Lifecycle.PER_METHOD)
//Prevent log messages from different tests to mix
@Execution(ExecutionMode.SAME_THREAD)
class DistributedJobDisconnectsAndRestartsTest : DjmTestSuite() {
    companion object : Logging

    @Test
    fun `during disconnect of DJM1, DJM2 does not steal WorkItem that currently under work by DJM1`() {
        val workItems = (1..10).map { it.toString() }.toSet()

        val job = object : DistributedJob {
            val isWorkItemConflictDetected = AtomicBoolean(false)
            val workShareInUse = ConcurrentHashMap.newKeySet<String>()
            override val jobId = JobId("job")
            override fun getSchedule() = DynamicProperty.of(Schedule.withRate(50))
            override fun run(context: DistributedJobContext) {
                for(workItem in context.workShare){
                    if(!workShareInUse.add(workItem)){
                        isWorkItemConflictDetected.set(true)
                    }
                }
                sleep(500)
                workShareInUse.removeAll(context.workShare)
            }
            override fun getWorkPool() = WorkPool.of(workItems)
            override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()
            override fun getWorkPoolCheckPeriod() = 0L
        }

        val rootPath = generateDjmRootPath()
        val djm1 = createDJM(job, rootPath = rootPath)
        val djm2 = createDJM(job, rootPath = rootPath)
        sleep(1000)
        job.isWorkItemConflictDetected.get().shouldBeFalse()

        disconnectDjm(djm1)
        sleep(1000)
        job.isWorkItemConflictDetected.get().shouldBeFalse()

        connectDjm(djm1)
        sleep(1000)
        job.isWorkItemConflictDetected.get().shouldBeFalse()

        closeDjm(djm1)
        sleep(1000)
        job.isWorkItemConflictDetected.get().shouldBeFalse()

        closeDjm(djm2)
        sleep(500)
        job.isWorkItemConflictDetected.get().shouldBeFalse()
    }

    @Disabled("TODO")
    @Test
    fun `when DJM3 disconnects, WorkItems rebalanced between DJM1 and DJM2`() {
        sleep(1000)
        TODO()
    }

    @Disabled("TODO")
    @Test
    fun `when DJM3 shutdowns, WorkItems rebalanced between DJM1 and DJM2`() {
        sleep(1000)
        TODO()
    }

    @Disabled("TODO")
    @Test
    fun `series of random DJMs disconnects, shutdowns, launches, WorkPool changes and restarts does not affect correct WorkItem launching and schedulling`() {
        sleep(1000)
        TODO("same workItem running only by single thread within the cluster")
        TODO("all workItem runs by schedule as expected with small disturbances")

    }

    @Disabled("TODO")
    @Test
    fun `series of DJMs restarts one by one does not affect correct WorkItem launching and schedulling`() {
        sleep(1000)
        TODO()
    }

    @Disabled("TODO")
    @Test
    fun `series of DJMs restarts two by two does not affect correct WorkItem launching and schedulling`() {
        sleep(1000)
        TODO()
    }

    @Disabled("TODO")
    @Test
    fun `series of DJMs restarts three by three does not affect correct WorkItem launching and schedulling`() {
        sleep(1000)
        TODO()
    }

    @Disabled("TODO")
    @Test
    fun `Change in Job WorkPool triggers rebalance`() {
        sleep(1000)
        TODO()
    }

    @Disabled("TODO")
    @Test
    fun `Check WorkPool period small, no change in WorkPool then should be no change to zk and no rebalance is triggered`() {
        sleep(1000)
        TODO()
    }

    @Disabled("TODO")
    @Test
    fun `If WorkPool and number of DJMs does not change, no rebalance is triggered `() {
        sleep(1000)
        TODO()
    }

    @Disabled("TODO")
    @Test
    fun `DJM shutdown triggers rebalance in cluster `() {
        sleep(1000)
        TODO()
    }


    @Disabled("TODO")
    @Test
    fun `DJM set of available jobs changes triggers rebalance in cluster `() {
        sleep(1000)
        TODO()

    }

    @Disabled("TODO")
    @Test
    fun `DJM follows assignment strategy`() {
        sleep(1000)
        TODO()
    }

    @Disabled("TODO")
    @Test
    fun `Assignment strategy that assign same workItem to different workers rise an exception`() {
        sleep(1000)
        TODO()
    }

    @Disabled("TODO")
    @Test
    fun `DJM does not allow two jobs with same ID`() {
        sleep(1000)
        TODO()
    }

    @Disabled("TODO")
    @Test
    fun `DJM does not allow incorrect symbols in WorkPool`() {
        sleep(1000)
        TODO()
    }


}

