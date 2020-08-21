package ru.fix.distributed.job.manager.strategy

import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.collections.shouldNotContainAnyOf
import io.kotest.matchers.shouldBe
import org.apache.logging.log4j.kotlin.Logging
import org.junit.jupiter.api.Test
import ru.fix.distributed.job.manager.JobId
import ru.fix.distributed.job.manager.model.AssignmentState
import ru.fix.distributed.job.manager.model.Availability
import ru.fix.distributed.job.manager.model.WorkItem

class AssignmentStrategyUsageTest {

    companion object : Logging

    private val ussdJobId = JobId("ussd")
    private val ussdWorkItems = setOf(WorkItem("ussd", ussdJobId))

    private val smsJobId = JobId("sms")
    private val smsWorkItems = (1..3).map { WorkItem("sms$it", smsJobId) }.toSet()

    private val rebillJobId = JobId("rebill")
    private val rebillWorkItems = (1..7).map { WorkItem("rebill$it", rebillJobId) }.toSet()

    private val workers = arrayOf("worker-0", "worker-1", "worker-2", "worker-3")

    private val singleUssdItemAssignedToOneOfWorkers = object : AbstractAssignmentStrategy() {

        override fun reassignAndBalance(
                availability: Availability,
                prevAssignment: AssignmentState,
                currentAssignment: AssignmentState,
                itemsToAssign: MutableSet<WorkItem>
        ) {
            val ussdItem = ussdWorkItems.single()
            itemsToAssign.remove(ussdItem)

            if (prevAssignment.containsWorkItem(ussdItem)) {
                val workerFromPrevious = prevAssignment.getWorkerOfWorkItem(ussdItem)
                currentAssignment.addWorkItem(workerFromPrevious!!, ussdItem)
            } else {
                val lessBusyWorker = currentAssignment.getLessBusyWorker(availability[ussdItem.jobId])
                currentAssignment.addWorkItem(lessBusyWorker!!, ussdItem)
            }
        }
    }

    // Strategy assign work items on workers, which doesn't contains of any work item of ussd job
    private val multipleSmsItemsAssignedToWorkersWithoutUssd = object : AbstractAssignmentStrategy() {

        override fun reassignAndBalance(
                availability: Availability,
                prevAssignment: AssignmentState,
                currentAssignment: AssignmentState,
                itemsToAssign: MutableSet<WorkItem>
        ) {
            for ((jobId, workers) in availability) {
                val itemsToAssignForJob = getWorkItemsByJob(jobId, itemsToAssign)
                val availableWorkers = HashSet(workers)

                workers.forEach { workerId ->
                    currentAssignment.putIfAbsent(workerId!!, HashSet<WorkItem>())

                    // ignore worker, where ussd job was launched
                    if (currentAssignment.containsAnyWorkItemOfJob(workerId, ussdJobId)) {
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


    @Test
    fun `assign single ussd and multiple sms on 4 workers`() {
        val customStrategy = object : AbstractAssignmentStrategy() {
            override fun reassignAndBalance(
                    availability: Availability,
                    prevAssignment: AssignmentState,
                    currentAssignment: AssignmentState,
                    itemsToAssign: MutableSet<WorkItem>
            ) {
                val ussdItemsToAssign = itemsToAssign.filter { it.jobId == ussdJobId }.toMutableSet()
                singleUssdItemAssignedToOneOfWorkers.reassignAndBalance(
                        availability,
                        prevAssignment,
                        currentAssignment,
                        ussdItemsToAssign
                )

                val smsItemsToAssign = itemsToAssign.filter { it.jobId == smsJobId }.toMutableSet()
                multipleSmsItemsAssignedToWorkersWithoutUssd.reassignAndBalance(
                        availability,
                        prevAssignment,
                        currentAssignment,
                        smsItemsToAssign
                )

                itemsToAssign.removeIf { it.jobId in setOf(ussdJobId, smsJobId) }

                // reassign items of other jobs using evenly spread strategy
                AssignmentStrategies.EVENLY_SPREAD.reassignAndBalance(
                        availability,
                        prevAssignment,
                        currentAssignment,
                        itemsToAssign
                )
            }
        }

        val currentAssignment = AssignmentState()
        customStrategy.reassignAndBalance(
                availability = availability {
                    ussdJobId.id(*workers)
                    smsJobId.id(*workers)
                    rebillJobId.id(*workers)
                },
                prevAssignment = AssignmentState(),
                currentAssignment = currentAssignment,
                itemsToAssign = (ussdWorkItems + smsWorkItems + rebillWorkItems).toMutableSet()
        )


        logger.info(currentAssignment)

        val workersWithUssdItem = currentAssignment.filter { it.value.contains(ussdWorkItems.single()) }
        workersWithUssdItem.size.shouldBe(1)
        workersWithUssdItem.values.single().shouldNotContainAnyOf(smsWorkItems)

        currentAssignment.isBalanced().shouldBeTrue()
    }
}