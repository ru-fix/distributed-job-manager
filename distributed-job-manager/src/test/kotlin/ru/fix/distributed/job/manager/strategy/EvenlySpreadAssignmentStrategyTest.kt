package ru.fix.distributed.job.manager.strategy

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import ru.fix.distributed.job.manager.model.AssignmentState
import ru.fix.distributed.job.manager.model.JobId
import ru.fix.distributed.job.manager.model.WorkItem
import ru.fix.distributed.job.manager.model.WorkerId

import java.util.HashSet

import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue

internal class EvenlySpreadAssignmentStrategyTest {
    private var evenlySpread: EvenlySpreadAssignmentStrategy? = null

    @BeforeEach
    fun setUp() {
        evenlySpread = EvenlySpreadAssignmentStrategy()
    }

    @Test
    fun reassignAndBalanceWhenOnlyOneWorkerHasJobs() {
        val available = AssignmentState()
        val previous = AssignmentState()

        available.addWorkItem(WorkerId("worker-0"), WorkItem("work-item-0", JobId("job-0")))
        available.addWorkItem(WorkerId("worker-0"), WorkItem("work-item-1", JobId("job-0")))
        available.addWorkItem(WorkerId("worker-0"), WorkItem("work-item-2", JobId("job-0")))
        available.addWorkItem(WorkerId("worker-0"), WorkItem("work-item-0", JobId("job-1")))
        available.addWorkItem(WorkerId("worker-0"), WorkItem("work-item-1", JobId("job-1")))
        available[WorkerId("worker-1")] = HashSet()
        available[WorkerId("worker-2")] = HashSet()

        previous[WorkerId("worker-0")] = HashSet()
        previous[WorkerId("worker-1")] = HashSet()
        previous[WorkerId("worker-2")] = HashSet()

        assertFalse(available.isBalanced)

        val newAssignment = evenlySpread!!.reassignAndBalance(
                generateAvailability(available),
                previous,
                AssignmentState(),
                generateItemsToAssign(available)
        )

        assertTrue(newAssignment.isBalanced)
    }

    @Test
    fun reassignAndBalanceWhenSomeWorkersHasJobs() {
        val available = AssignmentState()
        val previous = AssignmentState()

        addWorkerWithItems(available, "worker-0", 1, 3)
        available.addWorkItem(WorkerId("worker-1"), WorkItem("work-item-1", JobId("job-3")))
        available.addWorkItem(WorkerId("worker-1"), WorkItem("work-item-2", JobId("job-3")))
        available.addWorkItem(WorkerId("worker-1"), WorkItem("work-item-0", JobId("job-3")))
        available[WorkerId("worker-2")] = HashSet()

        previous[WorkerId("worker-0")] = HashSet()
        previous[WorkerId("worker-1")] = HashSet()
        previous[WorkerId("worker-2")] = HashSet()

        assertFalse(available.isBalanced)

        val newAssignment = evenlySpread!!.reassignAndBalance(
                generateAvailability(available),
                previous,
                AssignmentState(),
                generateItemsToAssign(available)
        )

        assertTrue(newAssignment.isBalanced)
    }

    @Test
    fun reassignAndBalanceIfWorkerNotAvailable() {
        val available = AssignmentState()
        val previous = AssignmentState()

        addWorkerWithItems(available, "worker-0", 3, 1)

        previous.addWorkItems(WorkerId("worker-1"), setOf(
                WorkItem("work-item-2", JobId("job-3")),
                WorkItem("work-item-0", JobId("job-3"))
        ))
        previous.addWorkItems(WorkerId("worker-0"), setOf(
                WorkItem("work-item-1", JobId("job-3")),
                WorkItem("work-item-0", JobId("job-0"))
        ))

        val newAssignment = evenlySpread!!.reassignAndBalance(
                generateAvailability(available),
                previous,
                AssignmentState(),
                generateItemsToAssign(available)
        )

        assertTrue(newAssignment.isBalanced)
    }

    @Test
    fun reassignAndBalanceIfNewWorkersAdded() {
        val available = AssignmentState()
        val previous = AssignmentState()

        addWorkerWithItems(available, "worker-0", 3, 1)
        addWorkerWithItems(available, "worker-2", 3, 1)
        addWorkerWithItems(available, "worker-3", 3, 1)
        available.addWorkItem(WorkerId("worker-1"), WorkItem("work-item-1", JobId("job-3")))
        available.addWorkItem(WorkerId("worker-1"), WorkItem("work-item-2", JobId("job-3")))
        available.addWorkItem(WorkerId("worker-1"), WorkItem("work-item-0", JobId("job-3")))

        previous.addWorkItems(WorkerId("worker-0"), setOf(
                WorkItem("work-item-1", JobId("job-3")),
                WorkItem("work-item-0", JobId("job-0"))
        ))
        previous.addWorkItems(WorkerId("worker-1"), setOf(
                WorkItem("work-item-0", JobId("job-3")),
                WorkItem("work-item-3", JobId("job-0"))
        ))

        val newAssignment = evenlySpread!!.reassignAndBalance(
                generateAvailability(available),
                previous,
                AssignmentState(),
                generateItemsToAssign(available)
        )

        assertTrue(newAssignment.isBalanced)
    }

    @Test
    fun reassignAndBalanceIfWorkerNotAvailableAndNewWorkerAdded() {
        val available = AssignmentState()
        val previous = AssignmentState()

        addWorkerWithItems(available, "worker-0", 3, 1)
        addWorkerWithItems(available, "worker-1", 3, 1)
        available.addWorkItem(WorkerId("worker-1"), WorkItem("work-item-1", JobId("job-3")))
        available.addWorkItem(WorkerId("worker-1"), WorkItem("work-item-2", JobId("job-3")))
        available.addWorkItem(WorkerId("worker-1"), WorkItem("work-item-0", JobId("job-3")))

        // Previous state contains worker-2 instead of worker-1.
        // It's emulate case, when worker-1 is not available, and worker-2 connected
        previous.addWorkItems(WorkerId("worker-0"), setOf(
                WorkItem("work-item-1", JobId("job-3")),
                WorkItem("work-item-0", JobId("job-0"))
        ))
        previous.addWorkItems(WorkerId("worker-2"), setOf(
                WorkItem("work-item-0", JobId("job-3")),
                WorkItem("work-item-2", JobId("job-0")),
                WorkItem("work-item-1", JobId("job-0")),
                WorkItem("work-item-2", JobId("job-3"))
        ))

        val newAssignment = evenlySpread!!.reassignAndBalance(
                generateAvailability(available),
                previous,
                AssignmentState(),
                generateItemsToAssign(available)
        )

        assertTrue(newAssignment.isBalanced)
    }
}