package ru.fix.distributed.job.manager.strategy

import io.kotest.assertions.withClue
import io.kotest.matchers.shouldBe
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import ru.fix.distributed.job.manager.model.AssignmentState
import java.util.stream.Stream

class AssignmentStrategyAcceptanceTest {

    companion object {
        @JvmStatic
        fun strategies(): Stream<Arguments> {
            return Stream.of(
                    Arguments.of(AssignmentStrategies.EVENLY_RENDEZVOUS),
                    Arguments.of(AssignmentStrategies.RENDEZVOUS),
                    Arguments.of(AssignmentStrategies.EVENLY_SPREAD)
            )
        }
    }


    @ParameterizedTest
    @MethodSource("strategies")
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
    @MethodSource("strategies")
    fun `one WorkItem can be assigned only once to particular worker`(
            strategy: AssignmentStrategy
    ) {
        val available = availability {
            "job-A"("worker-0", "worker-1", "worker-2")
        }
        val previous = assignmentState {
            "worker-0"{}
            "worker-1"{}
            "worker-2"{}
        }
        val workItems = workItems {
            "job-A"(*(1..10).map { "A$it" }.toTypedArray())
        }

        val current = AssignmentState()
        strategy.reassignAndBalance(available, previous, current, workItems)

        for (workItem in workItems) {
            withClue("WorkItem $workItem assigned once") {
                current.values.flatMap { it }.count().shouldBe(1)
            }
        }
    }
}