package ru.fix.distributed.job.manager.example

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import ru.fix.aggregating.profiler.AggregatingProfiler
import ru.fix.distributed.job.manager.*
import ru.fix.distributed.job.manager.model.*
import ru.fix.distributed.job.manager.strategy.AbstractAssignmentStrategy
import ru.fix.distributed.job.manager.strategy.AssignmentStrategies
import ru.fix.distributed.job.manager.strategy.AssignmentStrategy
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule

class RebillJob : DistributedJob {
    override fun getJobId(): String {
        return "rebill-job"
    }

    override fun getSchedule(): DynamicProperty<Schedule>? {
        return null
    }

    @Throws(Exception::class)
    override fun run(context: DistributedJobContext) {

    }

    override fun getWorkPool(): WorkPool? {
        return null
    }

    override fun getWorkPoolRunningStrategy(): WorkPoolRunningStrategy? {
        return null
    }

    override fun getWorkPoolCheckPeriod(): Long {
        return 0
    }
    //...
}

class SmsJob : DistributedJob {
    override fun getJobId(): String {
        return "sms-job"
    }

    override fun getSchedule(): DynamicProperty<Schedule>? {
        return null
    }

    @Throws(Exception::class)
    override fun run(context: DistributedJobContext) {

    }

    override fun getWorkPool(): WorkPool? {
        return null
    }

    override fun getWorkPoolRunningStrategy(): WorkPoolRunningStrategy? {
        return null
    }

    override fun getWorkPoolCheckPeriod(): Long {
        return 0
    }
    // ...
}

class UssdJob : DistributedJob {
    override fun getJobId(): String {
        return "ussd-job"
    }

    override fun getSchedule(): DynamicProperty<Schedule>? {
        return null
    }

    @Throws(Exception::class)
    override fun run(context: DistributedJobContext) {

    }

    override fun getWorkPool(): WorkPool? {
        return null
    }

    override fun getWorkPoolRunningStrategy(): WorkPoolRunningStrategy? {
        return null
    }

    override fun getWorkPoolCheckPeriod(): Long {
        return 0
    }
}

private val ussdAssignmentStrategy = object : AbstractAssignmentStrategy() {

    override fun reassignAndBalance(
            availability: MutableMap<JobId, MutableSet<WorkerId>>,
            prevAssignment: AssignmentState,
            currentAssignment: AssignmentState,
            itemsToAssign: MutableSet<WorkItem>
    ): AssignmentState {
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
        return currentAssignment
    }
}

// Strategy assign work items on workers, which doesn't contains of any work item of ussd job
private val smsAssignmentStrategy = object : AbstractAssignmentStrategy() {

    override fun reassignAndBalance(
            availability: MutableMap<JobId, MutableSet<WorkerId>>,
            prevAssignment: AssignmentState,
            currentAssignment: AssignmentState,
            itemsToAssign: MutableSet<WorkItem>
    ): AssignmentState {
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
        return currentAssignment
    }
}

class CustomAssignmentStrategy : AssignmentStrategy {
    override fun reassignAndBalance(
            availability: MutableMap<JobId, MutableSet<WorkerId>>,
            prevAssignment: AssignmentState,
            currentAssignment: AssignmentState,
            itemsToAssign: MutableSet<WorkItem>
    ): AssignmentState {
        var newState = ussdAssignmentStrategy.reassignAndBalance(
                mutableMapOf(JobId("ussd-job") to availability[JobId("ussd-job")]!!),
                prevAssignment,
                currentAssignment,
                itemsToAssign
        )
        availability.remove(JobId("ussd-job"))

        newState = smsAssignmentStrategy.reassignAndBalance(
                mutableMapOf(JobId("sms-job") to availability[JobId("sms-job")]!!),
                prevAssignment,
                newState,
                itemsToAssign
        )
        availability.remove(JobId("sms-job"))

        // reassign items of other jobs using evenly spread strategy
        return AssignmentStrategies.EVENLY_SPREAD.reassignAndBalance(
                availability,
                prevAssignment,
                newState,
                itemsToAssign
        )
    }
}

fun main() {
    DistributedJobManager(
            CuratorFrameworkFactory.newClient("list/of/servers", ExponentialBackoffRetry(1000, 10)),
            listOf(SmsJob(), UssdJob(), RebillJob()),
            AggregatingProfiler(),
            DistributedJobManagerSettings(
                    nodeId = "my-app-instance-1",
                    rootPath = "zk/root/path",
                    assignmentStrategy = CustomAssignmentStrategy(),
                    timeToWaitTermination = DynamicProperty.of(180_000L),
                    workPoolCleanPeriod = DynamicProperty.of(1_000L)
            )
    )
}


