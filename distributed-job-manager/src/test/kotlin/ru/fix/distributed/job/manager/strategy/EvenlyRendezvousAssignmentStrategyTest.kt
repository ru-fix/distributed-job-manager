package ru.fix.distributed.job.manager.strategy

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.fix.distributed.job.manager.JobId
import ru.fix.distributed.job.manager.model.AssignmentState
import ru.fix.distributed.job.manager.model.WorkerId

internal class EvenlyRendezvousAssignmentStrategyTest {
    private lateinit var evenlyRendezvous: EvenlyRendezvousAssignmentStrategy

    companion object {
        val logger: Logger = LoggerFactory.getLogger(EvenlyRendezvousAssignmentStrategyTest::class.java)
    }

    @BeforeEach
    fun setUp() {
        evenlyRendezvous = EvenlyRendezvousAssignmentStrategy()
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
        assertEquals(6, newAssignment.globalPoolSize())
        assertTrue(newAssignment.isBalanced)
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
        assertEquals(previous.globalPoolSize(), newAssignment.globalPoolSize())

        val workPoolSizeJob1 = previous.localPoolSize(JobId("job-1"))
        val workPoolSizeJob1OnWorker1 = newAssignment.getWorkItems(WorkerId("worker-1"), JobId("job-1")).size
        assertEquals(workPoolSizeJob1, newAssignment.localPoolSize(JobId("job-1")))
        assertEquals(workPoolSizeJob1, workPoolSizeJob1OnWorker1)
        assertTrue(newAssignment.isBalancedForEachJob(generateAvailability(available)))
        assertTrue(newAssignment.isBalanced)
    }

    @Test
    fun `items balanced, when worker-2 died and worker-3 started`() {
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
        assertEquals(previous.globalPoolSize(), newAssignment.globalPoolSize())
        assertTrue(newAssignment.isBalancedForEachJob(generateAvailability(available)))
        assertTrue(newAssignment.isBalanced)
    }

    @Test
    fun `balance items, when only worker-0 has work items`() {
        val available = assignmentState {
            "worker-0"(workPool)
            "worker-1"{}
            "worker-2"{}
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
        available: AssignmentState,
        previous: AssignmentState
    ): AssignmentState {
        val availability = generateAvailability(available)
        val itemsToAssign = generateItemsToAssign(available)

        logger.info(
            Report(
                availability,
                itemsToAssign,
                previous
            ).toString()
        )

        val newState = AssignmentState()
        evenlyRendezvous.reassignAndBalance(
            availability,
            previous,
            newState,
            itemsToAssign
        )
        logger.info(Report(newAssignment = newState).toString())
        return newState
    }
}