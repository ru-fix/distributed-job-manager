package ru.fix.distributed.job.manager.strategy

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.fix.distributed.job.manager.model.AssignmentState
import ru.fix.distributed.job.manager.model.JobId
import ru.fix.distributed.job.manager.model.WorkItem
import ru.fix.distributed.job.manager.model.WorkerId

internal class ReassignmentNumberComparisonTest {
    private lateinit var evenlySpread: EvenlySpreadAssignmentStrategy
    private lateinit var rendezvous: RendezvousHashAssignmentStrategy
    private lateinit var availableWorkPoolsMap: Map<Int, Int>

    companion object {
        val logger: Logger = LoggerFactory.getLogger(ReassignmentNumberComparisonTest::class.java)
    }

    @BeforeEach
    fun setUp() {
        evenlySpread = EvenlySpreadAssignmentStrategy()
        rendezvous = RendezvousHashAssignmentStrategy()
        availableWorkPoolsMap = sortedMapOf(32 to 1, 4 to 2, 5 to 3, 2 to 16, 1 to 285)
    }

    private class Results(
            internal val reassignmentNumber: Int,
            internal val newAssignment: AssignmentState
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

        assertEquals(1, reassignmentResults(available, previous, AssignmentStrategies.EVENLY_SPREAD).reassignmentNumber)
        assertEquals(2, reassignmentResults(available, previous, AssignmentStrategies.RENDEZVOUS).reassignmentNumber)
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

        assertEquals(1, reassignmentResults(available, previous, AssignmentStrategies.EVENLY_SPREAD).reassignmentNumber)
        assertEquals(2, reassignmentResults(available, previous, AssignmentStrategies.RENDEZVOUS).reassignmentNumber)
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

        assertEquals(0, reassignmentResults(available, previous, AssignmentStrategies.EVENLY_SPREAD).reassignmentNumber)
        assertEquals(2, reassignmentResults(available, previous, AssignmentStrategies.RENDEZVOUS).reassignmentNumber)
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

        assertEquals(2, reassignmentResults(available, previous, AssignmentStrategies.EVENLY_SPREAD).reassignmentNumber)
        assertEquals(3, reassignmentResults(available, previous, AssignmentStrategies.RENDEZVOUS).reassignmentNumber)
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

        assertEquals(3, reassignmentResults(available, previous, AssignmentStrategies.EVENLY_SPREAD).reassignmentNumber)
        assertEquals(5, reassignmentResults(available, previous, AssignmentStrategies.RENDEZVOUS).reassignmentNumber)
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

        assertEquals(2, reassignmentResults(available, previous, AssignmentStrategies.EVENLY_SPREAD).reassignmentNumber)
        assertEquals(6, reassignmentResults(available, previous, AssignmentStrategies.RENDEZVOUS).reassignmentNumber)
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

        assertEquals(10, reassignmentResults(available, previous, AssignmentStrategies.EVENLY_SPREAD).reassignmentNumber)
        assertEquals(10, reassignmentResults(available, previous, AssignmentStrategies.RENDEZVOUS).reassignmentNumber)
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

        assertEquals(2, reassignmentResults(available, previous, AssignmentStrategies.EVENLY_SPREAD).reassignmentNumber)
        assertEquals(4, reassignmentResults(available, previous, AssignmentStrategies.RENDEZVOUS).reassignmentNumber)
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

        assertEquals(3, reassignmentResults(available, previous, AssignmentStrategies.EVENLY_SPREAD).reassignmentNumber)
        assertEquals(10, reassignmentResults(available, previous, AssignmentStrategies.RENDEZVOUS).reassignmentNumber)
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

        var evenlySpreadResults = reassignmentResults(available, previous, AssignmentStrategies.EVENLY_SPREAD)
        var rendezvousResults = reassignmentResults(available, previous, AssignmentStrategies.RENDEZVOUS)
        assertEquals(14, evenlySpreadResults.reassignmentNumber)
        assertEquals(14, rendezvousResults.reassignmentNumber)

        available.remove(WorkerId("sws-4"))

        evenlySpreadResults = reassignmentResults(available, evenlySpreadResults.newAssignment, AssignmentStrategies.EVENLY_SPREAD)
        rendezvousResults = reassignmentResults(available, rendezvousResults.newAssignment, AssignmentStrategies.RENDEZVOUS)
        assertEquals(2, evenlySpreadResults.reassignmentNumber)
        assertEquals(2, rendezvousResults.reassignmentNumber)

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

        evenlySpreadResults = reassignmentResults(available, evenlySpreadResults.newAssignment, AssignmentStrategies.EVENLY_SPREAD)
        rendezvousResults = reassignmentResults(available, rendezvousResults.newAssignment, AssignmentStrategies.RENDEZVOUS)
        assertEquals(2, evenlySpreadResults.reassignmentNumber)
        assertEquals(2, rendezvousResults.reassignmentNumber)
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

        var evenlySpreadResults = reassignmentResults(available, previous, AssignmentStrategies.EVENLY_SPREAD)
        var rendezvousResults = reassignmentResults(available, previous, AssignmentStrategies.RENDEZVOUS)
        assertEquals(14, evenlySpreadResults.reassignmentNumber)
        assertEquals(14, rendezvousResults.reassignmentNumber)

        available.remove(WorkerId("sws-0"))
        evenlySpreadResults = reassignmentResults(available, evenlySpreadResults.newAssignment, AssignmentStrategies.EVENLY_SPREAD)
        rendezvousResults = reassignmentResults(available, rendezvousResults.newAssignment, AssignmentStrategies.RENDEZVOUS)
        assertEquals(2, evenlySpreadResults.reassignmentNumber)
        assertEquals(1, rendezvousResults.reassignmentNumber)

        available.remove(WorkerId("sws-1"))
        evenlySpreadResults = reassignmentResults(available, evenlySpreadResults.newAssignment, AssignmentStrategies.EVENLY_SPREAD)
        rendezvousResults = reassignmentResults(available, rendezvousResults.newAssignment, AssignmentStrategies.RENDEZVOUS)
        assertEquals(3, evenlySpreadResults.reassignmentNumber)
        assertEquals(1, rendezvousResults.reassignmentNumber)

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
        evenlySpreadResults = reassignmentResults(available, evenlySpreadResults.newAssignment, AssignmentStrategies.EVENLY_SPREAD)
        rendezvousResults = reassignmentResults(available, rendezvousResults.newAssignment, AssignmentStrategies.RENDEZVOUS)
        assertEquals(2, evenlySpreadResults.reassignmentNumber)
        assertEquals(1, rendezvousResults.reassignmentNumber)

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
        evenlySpreadResults = reassignmentResults(available, evenlySpreadResults.newAssignment, AssignmentStrategies.EVENLY_SPREAD)
        rendezvousResults = reassignmentResults(available, rendezvousResults.newAssignment, AssignmentStrategies.RENDEZVOUS)
        assertEquals(2, evenlySpreadResults.reassignmentNumber)
        assertEquals(1, rendezvousResults.reassignmentNumber)
    }

    @Test
    fun `start 8 workers in a row`() {
        val strategy = AssignmentStrategies.EVENLY_SPREAD
        var availableState: AssignmentState
        var previousState = AssignmentState()

        (1..8).forEach {
            availableState = generateAvailableState(availableWorkPoolsMap, it)
            assertEquals((32 * 1 + 4 * 2 + 5 * 3 + 2 * 16 + 1 * 285) * it, availableState.globalPoolSize())

            val availability = generateAvailability(availableState)
            val results = reassignmentResults(availableState, previousState, strategy, false)
            val newAssignment = results.newAssignment

            logger.info("NUMBER OF REASSIGNMENTS (Add worker-${it - 1}): ${results.reassignmentNumber}")
            logger.info(newAssignment.globalWorkPoolSizeInfo)

            assertTrue(newAssignment.isBalanced)
            assertTrue(newAssignment.isBalancedForEachJob(availability))

            previousState = newAssignment
        }
    }

    @Test
    fun `sequential reboot 8 workers`() {
        val strategy = AssignmentStrategies.EVENLY_SPREAD
        var availableState = generateAvailableState(availableWorkPoolsMap, 8)
        val previousState = reassignmentResults(availableState, AssignmentState(), strategy, false).newAssignment
        logger.info("Before sequential reboot of all servers:\n ${previousState.globalWorkPoolSizeInfo}")

        (0 until 8).forEach {
            availableState = generateAvailableState(
                    availableWorkPoolsMap,
                    (0 until 8).filter { i -> i != it }.toCollection(mutableSetOf<Int>())
            )
            var availability = generateAvailability(availableState)
            var results = reassignmentResults(availableState, previousState, strategy, false)
            var newAssignment = results.newAssignment

            logger.info("NUMBER OF REASSIGNMENTS (Remove worker-$it): ${results.reassignmentNumber}")
            assertTrue(newAssignment.isBalanced)
            assertTrue(newAssignment.isBalancedForEachJob(availability))

            availableState = generateAvailableState(availableWorkPoolsMap, 8)
            availability = generateAvailability(availableState)
            results = reassignmentResults(availableState, newAssignment, strategy, false)
            newAssignment = results.newAssignment

            logger.info("NUMBER OF REASSIGNMENTS (Add worker-$it): ${results.reassignmentNumber}")
            assertTrue(newAssignment.isBalanced)
            assertTrue(newAssignment.isBalancedForEachJob(availability))
        }
    }

    @Test
    fun `sequential double reboot 8 workers`() {
        val strategy = AssignmentStrategies.EVENLY_SPREAD
        var availableState = generateAvailableState(availableWorkPoolsMap, 8)
        val previousState = reassignmentResults(availableState, AssignmentState(), strategy, false).newAssignment
        logger.info("Before sequential double rebooting all servers:\n ${previousState.globalWorkPoolSizeInfo}")

        (0 until 4).forEach {
            availableState = generateAvailableState(
                    availableWorkPoolsMap,
                    (0 until 8).filter { i -> i != it * 2 }.toCollection(mutableSetOf<Int>())
            )
            var availability = generateAvailability(availableState)
            var results = reassignmentResults(availableState, previousState, strategy, false)
            var newAssignment = results.newAssignment

            logger.info("NUMBER OF REASSIGNMENTS (Remove worker-${it * 2}): ${results.reassignmentNumber}")
            assertTrue(newAssignment.isBalanced)
            assertTrue(newAssignment.isBalancedForEachJob(availability))

            availableState = generateAvailableState(
                    availableWorkPoolsMap,
                    (0 until 8).filter { i -> i != it * 2 + 1 }.toCollection(mutableSetOf<Int>())
            )
            availability = generateAvailability(availableState)
            results = reassignmentResults(availableState, previousState, strategy, false)
            newAssignment = results.newAssignment

            logger.info("NUMBER OF REASSIGNMENTS (Remove worker-${it * 2 + 1}): ${results.reassignmentNumber}")
            assertTrue(newAssignment.isBalanced)
            assertTrue(newAssignment.isBalancedForEachJob(availability))

            availableState = generateAvailableState(
                    availableWorkPoolsMap,
                    (0 until 8).filter { i -> i != it * 2 }.toCollection(mutableSetOf<Int>())
            )
            availability = generateAvailability(availableState)
            results = reassignmentResults(availableState, newAssignment, strategy, false)
            newAssignment = results.newAssignment

            logger.info("NUMBER OF REASSIGNMENTS (Add worker-${it * 2}): ${results.reassignmentNumber}")
            assertTrue(newAssignment.isBalanced)
            assertTrue(newAssignment.isBalancedForEachJob(availability))

            availableState = generateAvailableState(availableWorkPoolsMap, 8)
            availability = generateAvailability(availableState)
            results = reassignmentResults(availableState, newAssignment, strategy, false)
            newAssignment = results.newAssignment

            logger.info("NUMBER OF REASSIGNMENTS (Add worker-${it * 2 + 1}): ${results.reassignmentNumber}")
            assertTrue(newAssignment.isBalanced)
            assertTrue(newAssignment.isBalancedForEachJob(availability))
        }
    }

    @Test
    fun `shutdown all workers except one and start all 8 workers again`() {
        val strategy = AssignmentStrategies.EVENLY_SPREAD
        var availableState = generateAvailableState(availableWorkPoolsMap, 8)
        var previousState = reassignmentResults(availableState, AssignmentState(), strategy, false).newAssignment
        logger.info("Before shutdown all servers except one:\n ${previousState.globalWorkPoolSizeInfo}")

        // iteratively remove all workers except worker-0
        (7 downTo 1).forEach {
            availableState = generateAvailableState(
                    availableWorkPoolsMap,
                    (0 until 8).filter { i -> i < it }.toCollection(mutableSetOf<Int>())
            )
            val availability = generateAvailability(availableState)
            val results = reassignmentResults(availableState, previousState, strategy, false)
            val newAssignment = results.newAssignment

            logger.info("NUMBER OF REASSIGNMENTS (Remove worker-$it): ${results.reassignmentNumber}")
            logger.info(newAssignment.globalWorkPoolSizeInfo)
            assertTrue(newAssignment.isBalanced)
            assertTrue(newAssignment.isBalancedForEachJob(availability))

            previousState = newAssignment
        }

        // start workers-[1-7]
        (2..8).forEach {
            availableState = generateAvailableState(availableWorkPoolsMap, it)
            assertEquals((32 * 1 + 4 * 2 + 5 * 3 + 2 * 16 + 1 * 285) * it, availableState.globalPoolSize())

            val availability = generateAvailability(availableState)
            val results = reassignmentResults(availableState, previousState, strategy, false)
            val newAssignment = results.newAssignment

            logger.info("NUMBER OF REASSIGNMENTS (Add worker-${it - 1}): ${results.reassignmentNumber}")
            logger.info(newAssignment.globalWorkPoolSizeInfo)
            assertTrue(newAssignment.isBalanced)
            assertTrue(newAssignment.isBalancedForEachJob(availability))

            previousState = newAssignment
        }
    }

    private fun generateAvailableState(jobs: Map<Int, Int>, workerCount: Int): AssignmentState {
        val state = AssignmentState()
        val workPool = mutableSetOf<WorkItem>()

        var jobsShift = 0
        for ((jobCount, workPoolSize) in jobs) {
            (jobsShift until jobCount + jobsShift).forEach { jobNumber ->
                jobsShift++
                (0 until workPoolSize).forEach { workItemNumber ->
                    workPool.add(WorkItem("item-$workItemNumber", JobId("job-$jobNumber")))
                }
            }
        }
        (0 until workerCount).forEach {
            state.addWorkItems(WorkerId("worker-${it}"), workPool)
        }
        return state
    }

    private fun generateAvailableState(jobs: Map<Int, Int>, workerIds: Set<Int>): AssignmentState {
        val state = AssignmentState()
        val workPool = mutableSetOf<WorkItem>()

        var jobsShift = 0
        for ((jobCount, workPoolSize) in jobs) {
            (jobsShift until jobCount + jobsShift).forEach { jobNumber ->
                jobsShift++
                (0 until workPoolSize).forEach { workItemNumber ->
                    workPool.add(WorkItem("item-$workItemNumber", JobId("job-$jobNumber")))
                }
            }
        }
        workerIds.forEach {
            state.addWorkItems(WorkerId("worker-${it}"), workPool)
        }
        return state
    }

    private fun reassignmentResults(
            available: AssignmentState,
            previous: AssignmentState,
            strategy: AssignmentStrategy,
            logEnabled: Boolean = true
    ): Results {
        val availability = generateAvailability(available)
        val itemsToAssign = generateItemsToAssign(available)

        if (logEnabled) {
            logger.info(Report(
                    availability,
                    itemsToAssign,
                    previous).toString()
            )
        }

        val newAssignment = strategy.reassignAndBalance(
                availability,
                previous,
                AssignmentState(),
                itemsToAssign
        )
        if (logEnabled) {
            logger.info(Report(newAssignment = newAssignment).toString())
        }
        return Results(
                calculateReassignments(previous, newAssignment),
                newAssignment
        )
    }
}
