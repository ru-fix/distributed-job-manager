package ru.fix.distributed.job.manager.strategy

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import ru.fix.distributed.job.manager.model.AssignmentState
import ru.fix.distributed.job.manager.model.JobId
import ru.fix.distributed.job.manager.model.WorkItem
import ru.fix.distributed.job.manager.model.WorkerId
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
        Assertions.assertEquals(expected, current, "strategy: ${strategy.javaClass}")
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
            Assertions.assertEquals(1, current.values.flatMap { it }.count(),
                    "WorkItem $workItem assignment once")

            current[WorkerId("worker-0")]
        }
    }
}