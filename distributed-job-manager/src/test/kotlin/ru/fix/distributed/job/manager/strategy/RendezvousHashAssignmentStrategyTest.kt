package ru.fix.distributed.job.manager.strategy

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import ru.fix.distributed.job.manager.model.AssignmentState

internal class RendezvousHashAssignmentStrategyTest {
    private var rendezvous: RendezvousHashAssignmentStrategy? = null

    @BeforeEach
    fun setUp() {
        rendezvous = RendezvousHashAssignmentStrategy()
    }

    private val workPool: JobScope.() -> Unit = {
        "job-0"(
                "work-item-0",
                "work-item-1",
                "work-item-2",
                "work-item-3",
                "work-item-4",
                "work-item-5"
        )
    }
    private val workPool1: JobScope.() -> Unit = {
        "job-1"(
                "work-item-0",
                "work-item-1",
                "work-item-2"
        )
    }

    @Test
    fun `assign items when previous state empty`() {
        val available = assignmentState {
            "worker-0"(workPool)
            "worker-1"(workPool)
            "worker-2"(workPool)
        }
        val previous = assignmentState {
            "worker-0"{}
            "worker-1"{}
            "worker-2"{}
        }
        val newAssignment = calculateNewAssignment(available, previous)
        Assertions.assertEquals(6, newAssignment.globalPoolSize())
    }

    @Test
    fun `reassign items when new worker added`() {
        var available = assignmentState {
            "worker-0"(workPool)
            "worker-1"(workPool1)
            "worker-2"(workPool)
        }
        var previous = assignmentState {
            "worker-0"{}
            "worker-1"{}
            "worker-2"{}
        }

        previous = calculateNewAssignment(available, previous)
        available = assignmentState {
            "worker-0"(workPool)
            "worker-1"(workPool1)
            "worker-2"(workPool)
            "worker-3"(workPool)
        }

        val newAssignment = calculateNewAssignment(available, previous)
        Assertions.assertEquals(previous.globalPoolSize(), newAssignment.globalPoolSize())
    }

    @Test
    fun reassignAndBalanceIfWorkerNotAvailable() {
        var available = assignmentState {
            "worker-0"(workPool)
            "worker-1"(workPool1)
        }
        var previous = assignmentState {
            "worker-0"{}
            "worker-1"{}
        }
        previous = calculateNewAssignment(available, previous)
        available = assignmentState {
            "worker-0"(workPool)
            "worker-1"(workPool1)
            "worker-2"(workPool)
        }

        val newAssignment = calculateNewAssignment(available, previous)
        Assertions.assertEquals(previous.globalPoolSize(), newAssignment.globalPoolSize())
    }

    @Test
    fun reassignAndBalanceIfNewWorkersAdded() {
        var available = assignmentState {
            "worker-0"(workPool)
            "worker-1"(workPool)
            "worker-2"(workPool)
        }
        var previous = assignmentState {
            "worker-0"{}
            "worker-1"{}
            "worker-2"{}
        }

        previous = calculateNewAssignment(available, previous)
        available = assignmentState {
            "worker-0"(workPool)
            "worker-1"(workPool)
            "worker-2"(workPool)
            "worker-3"(workPool)
        }
        println(previous)

        val newAssignment = calculateNewAssignment(available, previous)
        Assertions.assertEquals(previous.globalPoolSize(), newAssignment.globalPoolSize())
    }

    @Test
    fun reassignAndBalanceIfWorkerNotAvailableAndNewWorkerAdded() {
        var available = assignmentState {
            "worker-0"(workPool)
            "worker-1"(workPool)
            "worker-2"(workPool)
        }
        var previous = assignmentState {
            "worker-0"{}
            "worker-1"{}
            "worker-2"{}
        }

        previous = calculateNewAssignment(available, previous)
        available = assignmentState {
            "worker-0"(workPool)
            "worker-1"(workPool)
            "worker-3"(workPool)
        }
        val newAssignment = calculateNewAssignment(available, previous)
        Assertions.assertEquals(previous.globalPoolSize(), newAssignment.globalPoolSize())
    }

    private fun calculateNewAssignment(
            available: AssignmentState,
            previous: AssignmentState
    ): AssignmentState {
        return rendezvous!!.reassignAndBalance(
                generateAvailability(available),
                previous,
                AssignmentState(),
                generateItemsToAssign(available)
        )
    }
}