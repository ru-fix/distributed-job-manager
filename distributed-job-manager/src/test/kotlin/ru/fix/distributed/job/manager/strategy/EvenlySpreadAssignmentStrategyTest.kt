package ru.fix.distributed.job.manager.strategy

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.fix.distributed.job.manager.model.AssignmentState

internal class EvenlySpreadAssignmentStrategyTest {
    private var evenlySpread: EvenlySpreadAssignmentStrategy? = null

    companion object {
        val logger: Logger = LoggerFactory.getLogger(EvenlySpreadAssignmentStrategyTest::class.java)
    }

    @BeforeEach
    fun setUp() {
        evenlySpread = EvenlySpreadAssignmentStrategy()
    }

    private val workPool: JobScope.() -> Unit = {
        "job-0"(
                "work-item-0",
                "work-item-1",
                "work-item-2"
        )
    }
    private val workPool1: JobScope.() -> Unit = {
        "job-3"(
                "work-item-0",
                "work-item-1"
        )
    }

    @Test
    fun reassignAndBalanceWhenOnlyOneWorkerHasJobs() {
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
        assertTrue(newAssignment.isBalanced)
    }

    @Test
    fun reassignAndBalanceWhenSomeWorkersHasJobs() {
        val available = assignmentState {
            "worker-0"(workPool)
            "worker-1"(workPool1)
            "worker-2"(workPool)
        }
        val previous = assignmentState {
            "worker-0"{}
            "worker-1"{}
            "worker-2"{}
        }

        val newAssignment = calculateNewAssignment(available, previous)
        assertTrue(newAssignment.isBalanced)
    }

    @Test
    fun reassignAndBalanceIfWorkerNotAvailable() {
        val available = assignmentState {
            "worker-0"(workPool)
            "worker-1"(workPool1)
        }
        val previous = assignmentState {
            "worker-0"(workPool)
            "worker-1"(workPool1)
            "worker-2"(workPool)
        }

        val newAssignment = calculateNewAssignment(available, previous)
        assertTrue(newAssignment.isBalanced)
    }

    @Test
    fun reassignAndBalanceIfNewWorkersAdded() {
        val available = assignmentState {
            "worker-0"(workPool)
            "worker-1"(workPool1)
            "worker-2"(workPool)
        }
        val previous = assignmentState {
            "worker-0"(workPool)
            "worker-1"(workPool1)
        }

        val newAssignment = calculateNewAssignment(available, previous)
        assertTrue(newAssignment.isBalanced)
    }

    @Test
    fun reassignAndBalanceIfWorkerNotAvailableAndNewWorkerAdded() {
        val available = assignmentState {
            "worker-0"(workPool)
            "worker-1"(workPool1)
        }
        val previous = assignmentState {
            "worker-0"(workPool)
            "worker-2"(workPool1)
        }

        val newAssignment = calculateNewAssignment(available, previous)
        assertTrue(newAssignment.isBalanced)
    }

    private fun calculateNewAssignment(
            available: AssignmentState,
            previous: AssignmentState
    ): AssignmentState {
        val availability = generateAvailability(available)
        val itemsToAssign = generateItemsToAssign(available)

        logger.info(Print.Builder()
                .availability(availability)
                .itemsToAssign(itemsToAssign)
                .previousAssignment(previous)
                .build().toString()
        )
        val newState = evenlySpread!!.reassignAndBalance(
                availability,
                previous,
                AssignmentState(),
                itemsToAssign
        )
        logger.info(Print.Builder()
                .evenlySpreadNewAssignment(newState)
                .build().toString()
        )
        return newState
    }
}