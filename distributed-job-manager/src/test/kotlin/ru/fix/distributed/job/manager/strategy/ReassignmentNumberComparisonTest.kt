package ru.fix.distributed.job.manager.strategy

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.platform.commons.logging.Logger
import org.junit.platform.commons.logging.LoggerFactory
import ru.fix.distributed.job.manager.model.AssignmentState
import ru.fix.distributed.job.manager.model.JobId
import ru.fix.distributed.job.manager.model.WorkItem
import ru.fix.distributed.job.manager.model.WorkerId

internal class ReassignmentNumberComparisonTest {
    private var evenlySpread: EvenlySpreadAssignmentStrategy? = null
    private var rendezvous: RendezvousHashAssignmentStrategy? = null

    companion object {
        val logger: Logger = LoggerFactory.getLogger(ReassignmentNumberComparisonTest::class.java)
    }

    @BeforeEach
    fun setUp() {
        evenlySpread = EvenlySpreadAssignmentStrategy()
        rendezvous = RendezvousHashAssignmentStrategy()
    }

    private class Results(
            internal val evenlySpreadReassignmentNumber: Int,
            internal val rendezvousReassignmentNumber: Int
    )

    @Test
    fun balanceItemsOfSingleJobBetweenTwoWorkers() {
        val available = AssignmentState()
        val previous = AssignmentState()

        val workItems = generateWorkItems(JobId("job-0"), 0, 4)
        available.addWorkItems(WorkerId("worker-0"), workItems)
        available.addWorkItems(WorkerId("worker-1"), workItems)

        previous.addWorkItems(WorkerId("worker-0"), setOf(
                WorkItem("work-item-0", JobId("job-0")),
                WorkItem("work-item-1", JobId("job-0")),
                WorkItem("work-item-2", JobId("job-0"))
        ))
        previous.addWorkItems(WorkerId("worker-1"), setOf(
                WorkItem("work-item-3", JobId("job-0")))
        )

        val results = reassignmentResults(available, previous)
        assertEquals(1, results.evenlySpreadReassignmentNumber)
        assertEquals(2, results.rendezvousReassignmentNumber)
    }

    @Test
    fun balanceItemsOfSingleJobBetweenThreeWorkers() {
        val available = AssignmentState()
        val previous = AssignmentState()

        val workItems = generateWorkItems(JobId("job-0"), 0, 4)
        available.addWorkItems(WorkerId("worker-0"), workItems)
        available.addWorkItems(WorkerId("worker-1"), workItems)
        available.addWorkItems(WorkerId("worker-2"), workItems)

        previous.addWorkItems(WorkerId("worker-0"), setOf(
                WorkItem("work-item-0", JobId("job-0")),
                WorkItem("work-item-1", JobId("job-0")),
                WorkItem("work-item-2", JobId("job-0"))
        ))
        previous.addWorkItems(WorkerId("worker-1"), setOf(
                WorkItem("work-item-3", JobId("job-0")))
        )

        val results = reassignmentResults(available, previous)
        assertEquals(1, results.evenlySpreadReassignmentNumber)
        assertEquals(2, results.rendezvousReassignmentNumber)
    }

    @Test
    fun balanceAlreadyBalancedItems() {
        val available = AssignmentState()
        val previous = AssignmentState()

        val workItems = generateWorkItems(JobId("job-0"), 0, 6)
        available.addWorkItems(WorkerId("worker-0"), workItems)
        available.addWorkItems(WorkerId("worker-1"), workItems)
        available.addWorkItems(WorkerId("worker-2"), workItems)

        previous.addWorkItems(WorkerId("worker-0"), setOf(
                WorkItem("work-item-0", JobId("job-0")),
                WorkItem("work-item-1", JobId("job-0"))
        ))
        previous.addWorkItems(WorkerId("worker-1"), setOf(
                WorkItem("work-item-2", JobId("job-0")),
                WorkItem("work-item-3", JobId("job-0"))
        ))
        previous.addWorkItems(WorkerId("worker-2"), setOf(
                WorkItem("work-item-4", JobId("job-0")),
                WorkItem("work-item-5", JobId("job-0"))
        ))

        val results = reassignmentResults(available, previous)
        assertEquals(0, results.evenlySpreadReassignmentNumber)
        assertEquals(2, results.rendezvousReassignmentNumber)
    }

    @Test
    fun balanceItemsWhenWorkItemsOfJobNotBalanced() {
        val available = AssignmentState()
        val previous = AssignmentState()

        val workItems = setOf(
                WorkItem("work-item-0", JobId("job-0")),
                WorkItem("work-item-1", JobId("job-0")),
                WorkItem("work-item-2", JobId("job-1")),
                WorkItem("work-item-3", JobId("job-1")),
                WorkItem("work-item-4", JobId("job-1")),
                WorkItem("work-item-5", JobId("job-1"))
        )
        available.addWorkItems(WorkerId("worker-0"), workItems)
        available.addWorkItems(WorkerId("worker-1"), workItems)
        available.addWorkItems(WorkerId("worker-2"), workItems)

        previous.addWorkItems(WorkerId("worker-0"), setOf(
                WorkItem("work-item-0", JobId("job-0")),
                WorkItem("work-item-1", JobId("job-0"))
        ))
        previous.addWorkItems(WorkerId("worker-1"), setOf(
                WorkItem("work-item-2", JobId("job-1")),
                WorkItem("work-item-3", JobId("job-1"))
        ))
        previous.addWorkItems(WorkerId("worker-2"), setOf(
                WorkItem("work-item-4", JobId("job-1")),
                WorkItem("work-item-5", JobId("job-1"))
        ))

        val results = reassignmentResults(available, previous)
        assertEquals(2, results.evenlySpreadReassignmentNumber)
        assertEquals(3, results.rendezvousReassignmentNumber)
    }

    @Test
    fun balanceItemsOfThreeJobsWhenNewWorkerStarted() {
        val available = AssignmentState()
        val previous = AssignmentState()

        val workItems = setOf(
                WorkItem("work-item-0", JobId("job-0")),
                WorkItem("work-item-1", JobId("job-1")),
                WorkItem("work-item-2", JobId("job-1")),
                WorkItem("work-item-3", JobId("job-1")),
                WorkItem("work-item-4", JobId("job-1")),
                WorkItem("work-item-5", JobId("job-1")),
                WorkItem("work-item-6", JobId("job-1")),
                WorkItem("work-item-7", JobId("job-2")),
                WorkItem("work-item-8", JobId("job-2"))
        )
        available.addWorkItems(WorkerId("worker-0"), workItems)
        available.addWorkItems(WorkerId("worker-1"), workItems)
        available.addWorkItems(WorkerId("worker-2"), workItems)

        previous.addWorkItems(WorkerId("worker-0"), setOf(
                WorkItem("work-item-1", JobId("job-1")),
                WorkItem("work-item-2", JobId("job-1")),
                WorkItem("work-item-7", JobId("job-2")),
                WorkItem("work-item-3", JobId("job-1"))
        ))
        previous.addWorkItems(WorkerId("worker-1"), setOf(
                WorkItem("work-item-4", JobId("job-1")),
                WorkItem("work-item-5", JobId("job-1")),
                WorkItem("work-item-6", JobId("job-1")),
                WorkItem("work-item-7", JobId("job-2")),
                WorkItem("work-item-0", JobId("job-0"))

        ))

        val results = reassignmentResults(available, previous)
        assertEquals(3, results.evenlySpreadReassignmentNumber)
        assertEquals(6, results.rendezvousReassignmentNumber)
    }

    @Test
    fun balanceItemsOfFourJobsWhenNewWorkerStarted() {
        val available = AssignmentState()
        val previous = AssignmentState()

        val workItems = setOf(
                WorkItem("work-item-0", JobId("job-0")),
                WorkItem("work-item-1", JobId("job-0")),
                WorkItem("work-item-2", JobId("job-1")),
                WorkItem("work-item-3", JobId("job-1")),
                WorkItem("work-item-4", JobId("job-2")),
                WorkItem("work-item-5", JobId("job-2")),
                WorkItem("work-item-6", JobId("job-3")),
                WorkItem("work-item-7", JobId("job-3"))
        )
        available.addWorkItems(WorkerId("worker-0"), workItems)
        available.addWorkItems(WorkerId("worker-1"), workItems)
        available.addWorkItems(WorkerId("worker-2"), workItems)

        previous.addWorkItems(WorkerId("worker-0"), setOf(
                WorkItem("work-item-0", JobId("job-0")),
                WorkItem("work-item-2", JobId("job-1")),
                WorkItem("work-item-4", JobId("job-2")),
                WorkItem("work-item-6", JobId("job-3"))
        ))
        previous.addWorkItems(WorkerId("worker-1"), setOf(
                WorkItem("work-item-1", JobId("job-0")),
                WorkItem("work-item-3", JobId("job-1")),
                WorkItem("work-item-5", JobId("job-2")),
                WorkItem("work-item-6", JobId("job-3"))
        ))

        val results = reassignmentResults(available, previous)
        assertEquals(2, results.evenlySpreadReassignmentNumber)
        assertEquals(7, results.rendezvousReassignmentNumber)
    }

    @Test
    fun balanceItemsOfFourJobsWhenWasAliveSingleWorkerAndNewWorkerStarted() {
        val available = AssignmentState()
        val previous = AssignmentState()

        val workItems = setOf(
                WorkItem("work-item-0", JobId("job-0")),
                WorkItem("work-item-1", JobId("job-0")),
                WorkItem("work-item-2", JobId("job-1")),
                WorkItem("work-item-3", JobId("job-1")),
                WorkItem("work-item-4", JobId("job-2")),
                WorkItem("work-item-5", JobId("job-2")),
                WorkItem("work-item-6", JobId("job-3")),
                WorkItem("work-item-7", JobId("job-3"))
        )
        available.addWorkItems(WorkerId("worker-0"), workItems)
        available.addWorkItems(WorkerId("worker-1"), workItems)

        previous.addWorkItems(WorkerId("worker-0"), workItems)

        val results = reassignmentResults(available, previous)
        assertEquals(4, results.evenlySpreadReassignmentNumber)
        assertEquals(5, results.rendezvousReassignmentNumber)
    }

    @Test
    fun balanceItemsOfFourJobsWhenWasAliveSingleWorkerAndNewFiveWorkersStarted() {
        val available = AssignmentState()
        val previous = AssignmentState()

        val workItems = setOf(
                WorkItem("work-item-0", JobId("job-0")),
                WorkItem("work-item-1", JobId("job-0")),
                WorkItem("work-item-2", JobId("job-0")),
                WorkItem("work-item-3", JobId("job-0")),
                WorkItem("work-item-4", JobId("job-0")),
                WorkItem("work-item-5", JobId("job-1")),
                WorkItem("work-item-6", JobId("job-1")),
                WorkItem("work-item-7", JobId("job-1")),
                WorkItem("work-item-8", JobId("job-1")),
                WorkItem("work-item-9", JobId("job-1")),
                WorkItem("work-item-10", JobId("job-2")),
                WorkItem("work-item-11", JobId("job-3")),
                WorkItem("work-item-12", JobId("job-3"))
        )
        available.addWorkItems(WorkerId("worker-0"), workItems)
        available.addWorkItems(WorkerId("worker-1"), workItems)
        available.addWorkItems(WorkerId("worker-2"), workItems)
        available.addWorkItems(WorkerId("worker-3"), workItems)
        available.addWorkItems(WorkerId("worker-4"), workItems)
        available.addWorkItems(WorkerId("worker-5"), workItems)

        previous.addWorkItems(WorkerId("worker-0"), workItems)

        val results = reassignmentResults(available, previous)
        assertEquals(10, results.evenlySpreadReassignmentNumber)
        assertEquals(10, results.rendezvousReassignmentNumber)
    }


    @Test
    fun balanceItemsOfSingleJobWhenWorkerDestroyed() {
        val available = AssignmentState()
        val previous = AssignmentState()

        val workItems = setOf(
                WorkItem("work-item-0", JobId("job-1")),
                WorkItem("work-item-1", JobId("job-1")),
                WorkItem("work-item-2", JobId("job-1")),
                WorkItem("work-item-3", JobId("job-1")),
                WorkItem("work-item-4", JobId("job-1")),
                WorkItem("work-item-5", JobId("job-1"))
        )
        available.addWorkItems(WorkerId("worker-0"), workItems)
        available.addWorkItems(WorkerId("worker-1"), workItems)

        previous.addWorkItems(WorkerId("worker-0"), setOf(
                WorkItem("work-item-0", JobId("job-1")),
                WorkItem("work-item-1", JobId("job-1"))
        ))
        previous.addWorkItems(WorkerId("worker-1"), setOf(
                WorkItem("work-item-2", JobId("job-1")),
                WorkItem("work-item-3", JobId("job-1"))
        ))
        previous.addWorkItems(WorkerId("worker-2"), setOf(
                WorkItem("work-item-4", JobId("job-1")),
                WorkItem("work-item-5", JobId("job-1"))
        ))

        val results = reassignmentResults(available, previous)
        assertEquals(2, results.evenlySpreadReassignmentNumber)
        assertEquals(4, results.rendezvousReassignmentNumber)
    }

    @Test
    fun balanceItemsOfSomeJobWhenWasAliveThreeWorkersAndOneWorkerDestroyed() {
        val available = AssignmentState()
        val previous = AssignmentState()

        val workItems = setOf(
                WorkItem("work-item-0", JobId("job-0")),
                WorkItem("work-item-1", JobId("job-0")),
                WorkItem("work-item-2", JobId("job-0")),
                WorkItem("work-item-3", JobId("job-1")),
                WorkItem("work-item-4", JobId("job-1")),
                WorkItem("work-item-5", JobId("job-1")),
                WorkItem("work-item-6", JobId("job-1")),
                WorkItem("work-item-7", JobId("job-2")),
                WorkItem("work-item-8", JobId("job-3")),
                WorkItem("work-item-9", JobId("job-3")),
                WorkItem("work-item-10", JobId("job-3"))
        )
        available.addWorkItems(WorkerId("worker-0"), workItems)
        available.addWorkItems(WorkerId("worker-1"), workItems)

        previous.addWorkItems(WorkerId("worker-0"), setOf(
                WorkItem("work-item-0", JobId("job-0")),
                WorkItem("work-item-3", JobId("job-1")),
                WorkItem("work-item-6", JobId("job-1")),
                WorkItem("work-item-9", JobId("job-3"))
        ))
        previous.addWorkItems(WorkerId("worker-1"), setOf(
                WorkItem("work-item-1", JobId("job-0")),
                WorkItem("work-item-3", JobId("job-1")),
                WorkItem("work-item-7", JobId("job-2")),
                WorkItem("work-item-10", JobId("job-3"))
        ))
        previous.addWorkItems(WorkerId("worker-2"), setOf(
                WorkItem("work-item-2", JobId("job-0")),
                WorkItem("work-item-5", JobId("job-1")),
                WorkItem("work-item-8", JobId("job-3"))
        ))

        val results = reassignmentResults(available, previous)
        assertEquals(4, results.evenlySpreadReassignmentNumber)
        assertEquals(9, results.rendezvousReassignmentNumber)
    }

    private fun reassignmentResults(available: AssignmentState, previous: AssignmentState): Results {
        val availability = generateAvailability(available)
        val itemsToAssign = generateItemsToAssign(available)

        val newAssignmentEvenlySpread = evenlySpread!!.reassignAndBalance(
                availability,
                previous,
                AssignmentState(),
                itemsToAssign
        )
        val newAssignmentRendezvous = rendezvous!!.reassignAndBalance(
                availability,
                previous,
                AssignmentState(),
                itemsToAssign
        )
        logger.info {
            Print.Builder()
                    .availability(availability)
                    .itemsToAssign(itemsToAssign)
                    .currentAssignment(newAssignmentEvenlySpread)
                    .previousAssignment(previous)
                    .build().toString()
        }

        return Results(
                calculateReassignments(previous, newAssignmentEvenlySpread),
                calculateReassignments(previous, newAssignmentRendezvous)
        )
    }
}
