package ru.fix.distributed.job.manager.strategy

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.fix.distributed.job.manager.JobId
import ru.fix.distributed.job.manager.model.AssignmentState
import ru.fix.distributed.job.manager.model.WorkerId

internal class RendezvousHashAssignmentStrategyTest {
    private lateinit var rendezvous: RendezvousHashAssignmentStrategy

    companion object {
        val logger: Logger = LoggerFactory.getLogger(RendezvousHashAssignmentStrategyTest::class.java)
    }

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
        assertEquals(6, newAssignment.globalPoolSize())
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
        assertEquals(previous.globalPoolSize(), newAssignment.globalPoolSize())
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
        assertEquals(previous.globalPoolSize(), newAssignment.globalPoolSize())
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
        assertEquals(previous.globalPoolSize(), newAssignment.globalPoolSize())
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
        rendezvous.reassignAndBalance(
            availability,
            previous,
            newState,
            itemsToAssign
        )
        logger.info(Report(newAssignment = newState).toString())
        return newState
    }
}