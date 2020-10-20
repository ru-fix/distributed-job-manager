package ru.fix.distributed.job.manager.strategy

import io.kotest.assertions.withClue
import io.kotest.matchers.shouldBe
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import ru.fix.distributed.job.manager.model.AssignmentState
import java.util.concurrent.ThreadLocalRandom
import java.util.stream.Stream

class AssignmentStrategyAcceptanceTest {

    companion object {
        @JvmStatic
        fun allStrategies(): Stream<Arguments> {
            return Stream.of(
                Arguments.of(AssignmentStrategies.EVENLY_RENDEZVOUS),
                Arguments.of(AssignmentStrategies.RENDEZVOUS),
                Arguments.of(AssignmentStrategies.EVENLY_SPREAD)
            )
        }

        @JvmStatic
        fun locallyBalancedStrategies(): Stream<Arguments> {
            return Stream.of(
                Arguments.of(AssignmentStrategies.EVENLY_RENDEZVOUS),
                Arguments.of(AssignmentStrategies.EVENLY_SPREAD)
            )
        }
    }


    @ParameterizedTest
    @MethodSource("allStrategies")
    fun `if availability strongly restricts assignments then assignment equals to availability`(
        strategy: AssignmentStrategy
    ) {
        val available = availability {
            "job-A"("worker-0")
            "job-B"("worker-1")
            "job-C"("worker-2")
        }
        val previous = assignmentState {
            "worker-0"{}
            "worker-1"{}
            "worker-2"{}
        }
        val workItems = workItems {
            "job-A"("A0")
            "job-B"("B0")
            "job-C"("C0")
        }

        val expected = assignmentState {
            "worker-0"{
                "job-A"("A0")
            }
            "worker-1"{
                "job-B"("B0")
            }
            "worker-2"{
                "job-C"("C0")
            }
        }

        val current = AssignmentState()
        strategy.reassignAndBalance(available, previous, current, workItems)
        withClue("strategy: ${strategy.javaClass}") {
            current.shouldBe(expected)
        }

    }

    @ParameterizedTest
    @MethodSource("allStrategies")
    fun `in random set item can be assigned only once to particular worker`(strategy: AssignmentStrategy) =
        repeat(100) {
            val workLoad = RandomWorkload(50, 50, 50)

            val current = AssignmentState()
            strategy.reassignAndBalance(workLoad.available, workLoad.previous, current, workLoad.workItems)

            for (workItem in workLoad.workItems) {
                withClue("WorkItem $workItem assigned once") {
                    current.values.flatMap { it }.count().shouldBe(1)
                }
            }
        }

    @ParameterizedTest
    @MethodSource("locallyBalancedStrategies")
    fun `random set of items always balanced if availability allows`(strategy: AssignmentStrategy) = repeat(100) {
        val workLoad = RandomWorkload(50, 50, 50)

        val current = AssignmentState()
        strategy.reassignAndBalance(workLoad.available, workLoad.previous, current, workLoad.workItems)

        for (workItem in workLoad.workItems) {
            withClue("WorkItem $workItem assigned once") {
                current.values.flatten().count().shouldBe(1)
            }
        }

        current.isBalancedForEachJob(workLoad.available)
    }

    class RandomWorkload(val maxWorkers: Int, maxJobs: Int, maxItemsPerJob: Int) {
        private val random = ThreadLocalRandom.current()

        val workers = (1..random.nextInt(1, maxWorkers + 1)).map { "w$it" }
        val jobs = (1..random.nextInt(1, maxJobs + 1)).map { "w$it" }

        val available = availability {
            for (job in jobs) {
                job(*workers.toTypedArray())
            }
        }
        val previous = assignmentState {
            for (worker in workers) {
                worker {}
            }
        }
        val workItems = workItems {
            for (job in jobs) {
                job(*(1..random.nextInt(1, maxItemsPerJob + 1)).map { "wi$it" }.toTypedArray())
            }
        }
    }
}