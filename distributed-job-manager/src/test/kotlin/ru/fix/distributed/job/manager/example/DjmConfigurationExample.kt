package ru.fix.distributed.job.manager.example

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import ru.fix.aggregating.profiler.AggregatingProfiler
import ru.fix.distributed.job.manager.*
import ru.fix.distributed.job.manager.model.AssignmentState
import ru.fix.distributed.job.manager.model.Availability
import ru.fix.distributed.job.manager.model.DistributedJobManagerSettings
import ru.fix.distributed.job.manager.model.WorkItem
import ru.fix.distributed.job.manager.strategy.AbstractAssignmentStrategy
import ru.fix.distributed.job.manager.strategy.AssignmentStrategies
import ru.fix.distributed.job.manager.strategy.AssignmentStrategy
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule
import java.time.Duration

class RebillJob : DistributedJob {
    override val jobId = JobId("rebill-job")

    override fun getSchedule() = Schedule.withDelay(DynamicProperty.of(1000L))

    override fun run(context: DistributedJobContext) {
    }

    override fun getWorkPool(): WorkPool {
        return WorkPool.of((1..15).map { "task-$it" }.toSet())
    }

    override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()

    override fun getWorkPoolCheckPeriod() = 0L

    //...
}

class SmsJob : DistributedJob {
    override val jobId = JobId("sms-job")

    override fun getSchedule() = Schedule.withDelay(DynamicProperty.of(100L))

    override fun run(context: DistributedJobContext) {
    }

    override fun getWorkPool() = WorkPool.singleton()

    override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()

    override fun getWorkPoolCheckPeriod() = 0L
    // ...
}

class UssdJob : DistributedJob {
    override val jobId = JobId("ussd-job")

    override fun getSchedule() = Schedule.withDelay(DynamicProperty.of(0L))

    override fun run(context: DistributedJobContext) {
    }

    override fun getWorkPool() = WorkPool.singleton()

    override fun getWorkPoolRunningStrategy() = WorkPoolRunningStrategies.getSingleThreadStrategy()

    override fun getWorkPoolCheckPeriod() = 0L
}

private val ussdAssignmentStrategy = object : AbstractAssignmentStrategy() {

    override fun reassignAndBalance(
        availability: Availability,
        prevAssignment: AssignmentState,
        currentAssignment: AssignmentState,
        itemsToAssign: MutableSet<WorkItem>
    ) {
        for ((key, value) in availability) {
            val itemsToAssignForJob = getWorkItemsByJob(key, itemsToAssign)

            value.forEach { workerId -> currentAssignment.putIfAbsent(workerId, HashSet<WorkItem>()) }

            for (item in itemsToAssignForJob) {
                if (prevAssignment.containsWorkItem(item)) {
                    val workerFromPrevious = prevAssignment.getWorkerOfWorkItem(item)
                    currentAssignment.addWorkItem(workerFromPrevious, item)
                } else {
                    val lessBusyWorker = currentAssignment
                        .getLessBusyWorker(value)
                    currentAssignment.addWorkItem(lessBusyWorker, item)
                }
            }
        }
    }
}

// Strategy assign work items on workers, which doesn't contains of any work item of ussd job
private val smsAssignmentStrategy = object : AbstractAssignmentStrategy() {

    override fun reassignAndBalance(
        availability: Availability,
        prevAssignment: AssignmentState,
        currentAssignment: AssignmentState,
        itemsToAssign: MutableSet<WorkItem>
    ) {
        for ((key, value) in availability) {
            val itemsToAssignForJob = getWorkItemsByJob(key, itemsToAssign)
            val availableWorkers = HashSet(value)

            value.forEach { workerId ->
                currentAssignment.putIfAbsent(workerId, HashSet<WorkItem>())

                // ignore worker, where ussd job was launched
                if (currentAssignment.containsAnyWorkItemOfJob(workerId, JobId("distr-job-id-1"))) {
                    availableWorkers.remove(workerId)
                }
            }

            for (item in itemsToAssignForJob) {
                if (currentAssignment.containsWorkItem(item)) {
                    continue
                }

                val lessBusyWorker = currentAssignment
                    .getLessBusyWorker(availableWorkers)
                currentAssignment.addWorkItem(lessBusyWorker, item)
                itemsToAssign.remove(item)
            }
        }
    }
}

class CustomAssignmentStrategy : AssignmentStrategy {
    override fun reassignAndBalance(
        availability: Availability,
        prevAssignment: AssignmentState,
        currentAssignment: AssignmentState,
        itemsToAssign: MutableSet<WorkItem>
    ) {
        ussdAssignmentStrategy.reassignAndBalance(
            Availability.of(mutableMapOf(JobId("ussd-job") to availability[JobId("ussd-job")]!!)),
            prevAssignment,
            currentAssignment,
            itemsToAssign
        )
        availability.remove(JobId("ussd-job"))

        smsAssignmentStrategy.reassignAndBalance(
            Availability.of(mutableMapOf(JobId("sms-job") to availability[JobId("sms-job")]!!)),
            prevAssignment,
            currentAssignment,
            itemsToAssign
        )
        availability.remove(JobId("sms-job"))

        // reassign items of other jobs using evenly rendezvous strategy
        AssignmentStrategies.EVENLY_RENDEZVOUS.reassignAndBalance(
            availability,
            prevAssignment,
            currentAssignment,
            itemsToAssign
        )
    }
}

fun main() {
    DistributedJobManager(
        CuratorFrameworkFactory.newClient("list/of/servers", ExponentialBackoffRetry(1000, 10)),
        "my-app-instance-1",
        "zk/root/path",
        CustomAssignmentStrategy(),
        listOf(SmsJob(), UssdJob(), RebillJob()),
        AggregatingProfiler(),
        DynamicProperty.of(
            DistributedJobManagerSettings(
                timeToWaitTermination = Duration.ofMinutes(3),
                workPoolCleanPeriod = Duration.ofHours(3)
            )
        )
    )
}


