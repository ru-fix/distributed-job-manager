package ru.fix.distributed.job.manager.strategy

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.platform.commons.logging.Logger
import org.junit.platform.commons.logging.LoggerFactory
import ru.fix.distributed.job.manager.model.AssignmentState
import ru.fix.distributed.job.manager.model.WorkerId

internal class ReassignmentNumberComparisonTest {
    private lateinit var evenlySpread: EvenlySpreadAssignmentStrategy
    private lateinit var rendezvous: RendezvousHashAssignmentStrategy

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
            internal val rendezvousReassignmentNumber: Int,
            internal val rendezvousNewAssigment: AssignmentState,
            internal val evenlySpreadNewAssignment: AssignmentState
    )

    @Test
    fun `balance items when work pools of two workers unbalanced`() {
        val workPool: JobScope.() -> Unit = {
            "job-0"(
                    "work-item-0",
                    "work-item-1",
                    "work-item-2",
                    "work-item-3"
            )
        }
        val available = assignmentState {
            "worker-0"(workPool)
            "worker-1"(workPool)
        }

        val previous = assignmentState {
            "worker-0"{
                "job-0"(
                        "work-item-0",
                        "work-item-1",
                        "work-item-2"
                )
            }
            "worker-1"{
                "job-0"(
                        "work-item-3"
                )
            }
        }

        val results = reassignmentResults(available, previous)
        assertEquals(1, results.evenlySpreadReassignmentNumber)
        assertEquals(2, results.rendezvousReassignmentNumber)
    }

    @Test
    fun `balance items new worker available`() {
        val workPool: JobScope.() -> Unit = {
            "job-0"(
                    "work-item-0",
                    "work-item-1",
                    "work-item-2",
                    "work-item-3"
            )
        }
        val available = assignmentState {
            "worker-0"(workPool)
            "worker-1"(workPool)
            "worker-2"(workPool)
        }

        val previous = assignmentState {
            "worker-0"{
                "job-0"(
                        "work-item-0",
                        "work-item-1",
                        "work-item-2"
                )
            }
            "worker-1"{
                "job-0"(
                        "work-item-3"
                )
            }
        }

        val results = reassignmentResults(available, previous)
        assertEquals(1, results.evenlySpreadReassignmentNumber)
        assertEquals(2, results.rendezvousReassignmentNumber)
    }

    @Test
    fun `balance already balanced items`() {
        val workPool: JobScope.() -> Unit = {
            "job-0"(
                    "work-item-0",
                    "work-item-1",
                    "work-item-2",
                    "work-item-3",
                    "work-item-4",
                    "work-item-5"
            )
        }
        val available = assignmentState {
            "worker-0"(workPool)
            "worker-1"(workPool)
            "worker-2"(workPool)
        }

        val previous = assignmentState {
            "worker-0"{
                "job-0"(
                        "work-item-0",
                        "work-item-1"
                )
            }
            "worker-1"{
                "job-0"(
                        "work-item-2",
                        "work-item-3"
                )
            }
            "worker-2"{
                "job-0"(
                        "work-item-4",
                        "work-item-5"
                )
            }
        }

        val results = reassignmentResults(available, previous)
        assertEquals(0, results.evenlySpreadReassignmentNumber)
        assertEquals(2, results.rendezvousReassignmentNumber)
    }

    @Test
    fun `balance items when work pools of three workers unbalanced`() {
        val workPool: JobScope.() -> Unit = {
            "job-0"(
                    "work-item-0",
                    "work-item-1"
            )
            "job-1"(
                    "work-item-2",
                    "work-item-3",
                    "work-item-4",
                    "work-item-5"
            )
        }
        val available = assignmentState {
            "worker-0"(workPool)
            "worker-1"(workPool)
            "worker-2"(workPool)
        }

        val previous = assignmentState {
            "worker-0"{
                "job-0"(
                        "work-item-0",
                        "work-item-1"
                )
            }
            "worker-1"{
                "job-1"(
                        "work-item-2",
                        "work-item-3"
                )
            }
            "worker-2"{
                "job-1"(
                        "work-item-4",
                        "work-item-5"
                )
            }
        }

        val results = reassignmentResults(available, previous)
        assertEquals(2, results.evenlySpreadReassignmentNumber)
        assertEquals(3, results.rendezvousReassignmentNumber)
    }

    @Test
    fun `balance items when state assigned and new worker started`() {
        val workPool: JobScope.() -> Unit = {
            "job-0"(
                    "work-item-0"
            )
            "job-1"(
                    "work-item-1",
                    "work-item-2",
                    "work-item-3",
                    "work-item-4",
                    "work-item-5",
                    "work-item-6"
            )
            "job-2"(
                    "work-item-7",
                    "work-item-8"
            )
        }
        val available = assignmentState {
            "worker-0"(workPool)
            "worker-1"(workPool)
            "worker-2"(workPool)
        }

        val previous = assignmentState {
            "worker-0"{
                "job-1"(
                        "work-item-1",
                        "work-item-2",
                        "work-item-3"
                )
                "job-2"(
                        "work-item-7"
                )
            }
            "worker-1"{
                "job-0"(
                        "work-item-0"
                )
                "job-1"(
                        "work-item-4",
                        "work-item-5",
                        "work-item-6"
                )
                "job-2"(
                        "work-item-8"
                )
            }
        }

        val results = reassignmentResults(available, previous)
        assertEquals(3, results.evenlySpreadReassignmentNumber)
        assertEquals(5, results.rendezvousReassignmentNumber)
    }

    @Test
    fun `balance items of 4 jobs when new worker started`() {
        val workPool: JobScope.() -> Unit = {
            "job-0"(
                    "work-item-0",
                    "work-item-1"
            )
            "job-1"(
                    "work-item-2",
                    "work-item-3"
            )
            "job-2"(
                    "work-item-4",
                    "work-item-5"
            )
            "job-3"(
                    "work-item-6",
                    "work-item-7"
            )
        }
        val available = assignmentState {
            "worker-0"(workPool)
            "worker-1"(workPool)
            "worker-2"(workPool)
        }

        val previous = assignmentState {
            "worker-0"{
                "job-0"(
                        "work-item-0"
                )
                "job-1"(
                        "work-item-2"
                )
                "job-2"(
                        "work-item-4"
                )
                "job-3"(
                        "work-item-6"
                )
            }
            "worker-1"{
                "job-0"(
                        "work-item-1"
                )
                "job-1"(
                        "work-item-3"
                )
                "job-2"(
                        "work-item-5"
                )
                "job-3"(
                        "work-item-7"
                )
            }
        }

        val results = reassignmentResults(available, previous)
        assertEquals(2, results.evenlySpreadReassignmentNumber)
        assertEquals(6, results.rendezvousReassignmentNumber)
    }

    @Test
    fun `balance items of 4 jobs when one worker was alive and new five workers started`() {
        val workPool: JobScope.() -> Unit = {
            "job-0"(
                    "work-item-0",
                    "work-item-1",
                    "work-item-2",
                    "work-item-3",
                    "work-item-4"
            )
            "job-1"(
                    "work-item-5",
                    "work-item-6",
                    "work-item-7",
                    "work-item-8",
                    "work-item-9"
            )
            "job-2"(
                    "work-item-10"
            )
            "job-3"(
                    "work-item-11",
                    "work-item-12"
            )
        }
        val available = assignmentState {
            "worker-0"(workPool)
            "worker-1"(workPool)
            "worker-2"(workPool)
            "worker-3"(workPool)
            "worker-4"(workPool)
            "worker-5"(workPool)
        }

        val previous = assignmentState {
            "worker-0"(workPool)
        }

        val results = reassignmentResults(available, previous)
        assertEquals(10, results.evenlySpreadReassignmentNumber)
        assertEquals(10, results.rendezvousReassignmentNumber)
    }


    @Test
    fun `balance items when one worker destroyed`() {
        val workPool: JobScope.() -> Unit = {
            "job-1"(
                    "work-item-0",
                    "work-item-1",
                    "work-item-2",
                    "work-item-3",
                    "work-item-4",
                    "work-item-5"
            )
        }
        val available = assignmentState {
            "worker-0"(workPool)
            "worker-1"(workPool)
        }

        val previous = assignmentState {
            "worker-0"{
                "job-1"(
                        "work-item-0",
                        "work-item-1"
                )
            }
            "worker-1"{
                "job-1"(
                        "work-item-2",
                        "work-item-3"
                )
            }
            "worker-2"{
                "job-1"(
                        "work-item-4",
                        "work-item-5"
                )
            }
        }

        val results = reassignmentResults(available, previous)
        assertEquals(2, results.evenlySpreadReassignmentNumber)
        assertEquals(4, results.rendezvousReassignmentNumber)
    }

    @Test
    fun `balance items when three workers was alive and one worker destroyed`() {
        val workPool: JobScope.() -> Unit = {
            "job-0"(
                    "work-item-0",
                    "work-item-1",
                    "work-item-2"
            )
            "job-1"(
                    "work-item-3",
                    "work-item-4",
                    "work-item-5",
                    "work-item-6"
            )
            "job-2"(
                    "work-item-7"
            )
            "job-3"(
                    "work-item-8",
                    "work-item-9",
                    "work-item-10"
            )
        }
        val available = assignmentState {
            "worker-0"(workPool)
            "worker-1"(workPool)
        }

        val previous = assignmentState {
            "worker-0" {
                "job-0"(
                        "work-item-0"
                )
                "job-1"(
                        "work-item-3",
                        "work-item-6"
                )
                "job-3"(
                        "work-item-9"
                )
            }
            "worker-1" {
                "job-0"(
                        "work-item-1"
                )
                "job-1"(
                        "work-item-4"
                )
                "job-2"(
                        "work-item-7"
                )
                "job-3"(
                        "work-item-10"
                )
            }
            "worker-2" {
                "job-0"(
                        "work-item-2"
                )
                "job-1"(
                        "work-item-5"
                )
                "job-3"(
                        "work-item-8"
                )
            }
        }

        val results = reassignmentResults(available, previous)
        assertEquals(3, results.evenlySpreadReassignmentNumber)
        assertEquals(10, results.rendezvousReassignmentNumber)
    }

    @Test
    fun `reboot one worker`() {
        val workPoolForSws: JobScope.() -> Unit = {
            "rebill-job"(
                    "rebill-item-0",
                    "rebill-item-1",
                    "rebill-item-2",
                    "rebill-item-3",
                    "rebill-item-4",
                    "rebill-item-5",
                    "rebill-item-6",
                    "rebill-item-7"
            )
            "singleton-job-1"(
                    "all"
            )
            "singleton-job-2"(
                    "all"
            )
        }
        val workPoolForSmpp: JobScope.() -> Unit = {
            "ussd-job"(
                    "ussd-work-item"
            )
            "sms-job"(
                    "sms-work-item-0",
                    "sms-work-item-1",
                    "sms-work-item-2"
            )
        }
        var available = assignmentState {
            "sws-0"(workPoolForSws)
            "sws-1"(workPoolForSws)
            "sws-2"(workPoolForSws)
            "sws-3"(workPoolForSws)
            "sws-4"(workPoolForSws)

            "smpp-0"(workPoolForSmpp)
            "smpp-1"(workPoolForSmpp)
            "smpp-2"(workPoolForSmpp)
            "smpp-3"(workPoolForSmpp)
        }
        val previous = assignmentState {
            "sws-0"{}
            "sws-1"{}
            "sws-2"{}
            "sws-3"{}
            "sws-4"{}

            "smpp-0"{}
            "smpp-1"{}
            "smpp-2"{}
            "smpp-3"{}
        }

        var evenlySpreadResults = reassignmentResults(available, previous)
        var rendezvousResults = reassignmentResults(available, previous)
        assertEquals(14, evenlySpreadResults.evenlySpreadReassignmentNumber)
        assertEquals(14, rendezvousResults.rendezvousReassignmentNumber)

        available.remove(WorkerId("sws-4"))

        evenlySpreadResults = reassignmentResults(available, evenlySpreadResults.evenlySpreadNewAssignment)
        rendezvousResults = reassignmentResults(available, rendezvousResults.rendezvousNewAssigment)
        assertEquals(2, evenlySpreadResults.evenlySpreadReassignmentNumber)
        assertEquals(2, rendezvousResults.rendezvousReassignmentNumber)

        available = assignmentState {
            "sws-0"(workPoolForSws)
            "sws-1"(workPoolForSws)
            "sws-2"(workPoolForSws)
            "sws-3"(workPoolForSws)
            "sws-4"(workPoolForSws)

            "smpp-0"(workPoolForSmpp)
            "smpp-1"(workPoolForSmpp)
            "smpp-2"(workPoolForSmpp)
            "smpp-3"(workPoolForSmpp)
        }

        evenlySpreadResults = reassignmentResults(available, evenlySpreadResults.evenlySpreadNewAssignment)
        rendezvousResults = reassignmentResults(available, rendezvousResults.rendezvousNewAssigment)
        assertEquals(2, evenlySpreadResults.evenlySpreadReassignmentNumber)
        assertEquals(2, rendezvousResults.rendezvousReassignmentNumber)
    }

    @Test
    fun `reboot two workers`() {
        val workPoolForSws: JobScope.() -> Unit = {
            "rebill-job"(
                    "rebill-item-0",
                    "rebill-item-1",
                    "rebill-item-2",
                    "rebill-item-3",
                    "rebill-item-4",
                    "rebill-item-5",
                    "rebill-item-6",
                    "rebill-item-7"
            )
            "singleton-job-1"(
                    "all"
            )
            "singleton-job-2"(
                    "all"
            )
        }
        val workPoolForSmpp: JobScope.() -> Unit = {
            "ussd-job"(
                    "ussd-work-item"
            )
            "sms-job"(
                    "sms-work-item-0",
                    "sms-work-item-1",
                    "sms-work-item-2"
            )
        }
        var available = assignmentState {
            "sws-0"(workPoolForSws)
            "sws-1"(workPoolForSws)
            "sws-2"(workPoolForSws)
            "sws-3"(workPoolForSws)
            "sws-4"(workPoolForSws)

            "smpp-0"(workPoolForSmpp)
            "smpp-1"(workPoolForSmpp)
            "smpp-2"(workPoolForSmpp)
            "smpp-3"(workPoolForSmpp)
        }
        val previous = assignmentState {
            "sws-0"{}
            "sws-1"{}
            "sws-2"{}
            "sws-3"{}
            "sws-4"{}

            "smpp-0"{}
            "smpp-1"{}
            "smpp-2"{}
            "smpp-3"{}
        }

        var evenlySpreadResults = reassignmentResults(available, previous)
        var rendezvousResults = reassignmentResults(available, previous)
        assertEquals(14, evenlySpreadResults.evenlySpreadReassignmentNumber)
        assertEquals(14, rendezvousResults.rendezvousReassignmentNumber)

        available.remove(WorkerId("sws-0"))
        evenlySpreadResults = reassignmentResults(available, evenlySpreadResults.evenlySpreadNewAssignment)
        rendezvousResults = reassignmentResults(available, rendezvousResults.rendezvousNewAssigment)
        assertEquals(2, evenlySpreadResults.evenlySpreadReassignmentNumber)
        assertEquals(1, rendezvousResults.rendezvousReassignmentNumber)

        available.remove(WorkerId("sws-1"))
        evenlySpreadResults = reassignmentResults(available, evenlySpreadResults.evenlySpreadNewAssignment)
        rendezvousResults = reassignmentResults(available, rendezvousResults.rendezvousNewAssigment)
        assertEquals(3, evenlySpreadResults.evenlySpreadReassignmentNumber)
        assertEquals(1, rendezvousResults.rendezvousReassignmentNumber)

        available = assignmentState {
            "sws-1"(workPoolForSws)
            "sws-2"(workPoolForSws)
            "sws-3"(workPoolForSws)
            "sws-4"(workPoolForSws)

            "smpp-0"(workPoolForSmpp)
            "smpp-1"(workPoolForSmpp)
            "smpp-2"(workPoolForSmpp)
            "smpp-3"(workPoolForSmpp)
        }
        evenlySpreadResults = reassignmentResults(available, evenlySpreadResults.evenlySpreadNewAssignment)
        rendezvousResults = reassignmentResults(available, rendezvousResults.rendezvousNewAssigment)
        assertEquals(2, evenlySpreadResults.evenlySpreadReassignmentNumber)
        assertEquals(1, rendezvousResults.rendezvousReassignmentNumber)

        available = assignmentState {
            "sws-0"(workPoolForSws)
            "sws-1"(workPoolForSws)
            "sws-2"(workPoolForSws)
            "sws-3"(workPoolForSws)
            "sws-4"(workPoolForSws)

            "smpp-0"(workPoolForSmpp)
            "smpp-1"(workPoolForSmpp)
            "smpp-2"(workPoolForSmpp)
            "smpp-3"(workPoolForSmpp)
        }
        evenlySpreadResults = reassignmentResults(available, evenlySpreadResults.evenlySpreadNewAssignment)
        rendezvousResults = reassignmentResults(available, rendezvousResults.rendezvousNewAssigment)
        assertEquals(3, evenlySpreadResults.evenlySpreadReassignmentNumber)
        assertEquals(1, rendezvousResults.rendezvousReassignmentNumber)
    }

    private fun reassignmentResults(available: AssignmentState, previous: AssignmentState): Results {
        val availability = generateAvailability(available)
        val itemsToAssign = generateItemsToAssign(available)

        logger.info {
            Print.Builder()
                    .availability(availability)
                    .itemsToAssign(itemsToAssign)
                    .previousAssignment(previous)
                    .build().toString()
        }

        val newAssignmentEvenlySpread = evenlySpread.reassignAndBalance(
                availability,
                previous,
                AssignmentState(),
                itemsToAssign
        )
        val newAssignmentRendezvous = rendezvous.reassignAndBalance(
                availability,
                previous,
                AssignmentState(),
                generateItemsToAssign(available)
        )
        logger.info {
            Print.Builder()
                    .evenlySpreadNewAssignment(newAssignmentEvenlySpread)
                    .rendezvousNewAssignment(newAssignmentRendezvous)
                    .build().toString()
        }
        return Results(
                calculateReassignments(previous, newAssignmentEvenlySpread),
                calculateReassignments(previous, newAssignmentRendezvous),
                newAssignmentRendezvous,
                newAssignmentEvenlySpread
        )
    }
}
