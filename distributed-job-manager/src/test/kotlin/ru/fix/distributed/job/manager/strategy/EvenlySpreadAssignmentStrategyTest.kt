package ru.fix.distributed.job.manager.strategy

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.fix.distributed.job.manager.model.AssignmentState

internal class EvenlySpreadAssignmentStrategyTest {
    private lateinit var evenlySpread: EvenlySpreadAssignmentStrategy

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
        assertTrue(newAssignment.isBalancedForEachJob(generateAvailability(available)))
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
        assertTrue(newAssignment.isBalancedForEachJob(generateAvailability(available)))
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
        assertTrue(newAssignment.isBalancedForEachJob(generateAvailability(available)))
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
        assertTrue(newAssignment.isBalancedForEachJob(generateAvailability(available)))
        assertTrue(newAssignment.isBalanced)
    }

    @Test
    fun `reassign when new worker added and local work pools evenly reassigned, but not global`() {
        val workPool: JobScope.() -> Unit = {
            "job-0"(
                    "work-item-0",
                    "work-item-1",
                    "work-item-2"
            )
            "job-1"(
                    "work-item-0",
                    "work-item-1",
                    "work-item-2"
            )
            "job-2"(
                    "work-item-0",
                    "work-item-1",
                    "work-item-2"
            )
            "job-3"(
                    "work-item-0",
                    "work-item-1",
                    "work-item-2"
            )
        }
        val available = assignmentState {
            "worker-0"(workPool)
            "worker-1"(workPool)
            "worker-2"(workPool)
            "worker-3"(workPool)
        }
        val previous = assignmentState {
            "worker-0" {
                "job-0"(
                        "work-item-0"
                )
                "job-1"(
                        "work-item-0"
                )
                "job-2"(
                        "work-item-0"
                )
                "job-3"(
                        "work-item-0"
                )
            }
            "worker-1" {
                "job-0"(
                        "work-item-1"
                )
                "job-1"(
                        "work-item-1"
                )
                "job-2"(
                        "work-item-1"
                )
                "job-3"(
                        "work-item-1"
                )
            }
            "worker-2" {
                "job-0"(
                        "work-item-2"
                )
                "job-1"(
                        "work-item-2"
                )
                "job-2"(
                        "work-item-2"
                )
                "job-3"(
                        "work-item-2"
                )
            }
        }

        val newAssignment = calculateNewAssignment(available, previous)
        assertTrue(newAssignment.isBalancedForEachJob(generateAvailability(available)))
    }

    @Test
    fun `iteratively start four workers and reboot one worker`() {
        val workPool: JobScope.() -> Unit = {
            "job-0"(
                    "work-item-0",
                    "work-item-1",
                    "work-item-2",
                    "work-item-3",
                    "work-item-4",
                    "work-item-5",
                    "work-item-6"
            )
            "job-1"(
                    "work-item-0"
            )
            "job-2"(
                    "work-item-0"
            )
            "job-3"(
                    "work-item-0"
            )
        }
        var available = assignmentState {
            "worker-0"(workPool)
        }
        val previous = assignmentState {}

        var newAssignment = calculateNewAssignment(available, previous)
        assertTrue(newAssignment.isBalancedForEachJob(generateAvailability(available)))
        assertTrue(newAssignment.isBalanced)

        available = assignmentState {
            "worker-0"(workPool)
            "worker-1"(workPool)
        }
        newAssignment = calculateNewAssignment(available, newAssignment)
        assertTrue(newAssignment.isBalancedForEachJob(generateAvailability(available)))

        available = assignmentState {
            "worker-0"(workPool)
            "worker-1"(workPool)
            "worker-2"(workPool)
        }
        newAssignment = calculateNewAssignment(available, newAssignment)
        assertTrue(newAssignment.isBalancedForEachJob(generateAvailability(available)))

        available = assignmentState {
            "worker-0"(workPool)
            "worker-1"(workPool)
            "worker-2"(workPool)
            "worker-3"(workPool)
        }
        newAssignment = calculateNewAssignment(available, newAssignment)
        assertTrue(newAssignment.isBalancedForEachJob(generateAvailability(available)))

        // reboot worker-0
        available = assignmentState {
            "worker-1"(workPool)
            "worker-2"(workPool)
            "worker-3"(workPool)
        }
        newAssignment = calculateNewAssignment(available, newAssignment)
        assertTrue(newAssignment.isBalancedForEachJob(generateAvailability(available)))

        available = assignmentState {
            "worker-0"(workPool)
            "worker-1"(workPool)
            "worker-2"(workPool)
            "worker-3"(workPool)
        }
        newAssignment = calculateNewAssignment(available, newAssignment)
        assertTrue(newAssignment.isBalancedForEachJob(generateAvailability(available)))
    }

    @Test
    fun `iteratively start three workers and reboot two workers`() {
        val workPool: JobScope.() -> Unit = {
            "job-0"(
                    "work-item-0",
                    "work-item-1",
                    "work-item-2",
                    "work-item-3",
                    "work-item-4",
                    "work-item-5",
                    "work-item-6",
                    "work-item-7",
                    "work-item-8",
                    "work-item-9"
            )
            "job-1"(
                    "work-item-0",
                    "work-item-1",
                    "work-item-2"
            )
            "job-2"(
                    "work-item-0"
            )
            "job-3"(
                    "work-item-0"
            )
        }
        var available = assignmentState {
            "worker-0"(workPool)
        }
        val previous = assignmentState {}

        var newAssignment = calculateNewAssignment(available, previous)
        assertTrue(newAssignment.isBalancedForEachJob(generateAvailability(available)))
        assertTrue(newAssignment.isBalanced)

        available = assignmentState {
            "worker-0"(workPool)
            "worker-1"(workPool)
        }
        newAssignment = calculateNewAssignment(available, newAssignment)
        assertTrue(newAssignment.isBalancedForEachJob(generateAvailability(available)))

        available = assignmentState {
            "worker-0"(workPool)
            "worker-1"(workPool)
            "worker-2"(workPool)
        }
        newAssignment = calculateNewAssignment(available, newAssignment)
        assertTrue(newAssignment.isBalancedForEachJob(generateAvailability(available)))

        // shutdown worker-0
        available = assignmentState {
            "worker-1"(workPool)
            "worker-2"(workPool)
        }
        newAssignment = calculateNewAssignment(available, newAssignment)
        assertTrue(newAssignment.isBalancedForEachJob(generateAvailability(available)))

        // shutdown worker-1
        available = assignmentState {
            "worker-2"(workPool)
        }
        newAssignment = calculateNewAssignment(available, newAssignment)
        assertTrue(newAssignment.isBalancedForEachJob(generateAvailability(available)))

        // start again worker-0
        available = assignmentState {
            "worker-0"(workPool)
            "worker-2"(workPool)
        }
        newAssignment = calculateNewAssignment(available, newAssignment)
        assertTrue(newAssignment.isBalancedForEachJob(generateAvailability(available)))

        // start again worker-1
        available = assignmentState {
            "worker-0"(workPool)
            "worker-1"(workPool)
            "worker-2"(workPool)
        }
        newAssignment = calculateNewAssignment(available, newAssignment)
        assertTrue(newAssignment.isBalancedForEachJob(generateAvailability(available)))
    }

    private fun calculateNewAssignment(
            available: AssignmentState, previous: AssignmentState
    ): AssignmentState {
        val availability = generateAvailability(available)
        val itemsToAssign = generateItemsToAssign(available)

        logger.info(Report(
                availability,
                itemsToAssign,
                previous).toString()
        )
        val newState = AssignmentState()
        evenlySpread.reassignAndBalance(
                availability,
                previous,
                newState,
                itemsToAssign
        )
        logger.info(Report(newAssignment = newState).toString())
        return newState
    }
}