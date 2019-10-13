package ru.fix.distributed.job.manager

import org.junit.jupiter.api.Test
import ru.fix.distributed.job.manager.strategy.assignmentState

class AssignmentTest {

    @Test
    fun `assignment of `() {
        val current = assignmentState {
            "w1"{
                "j1"(
                        "wi1",
                        "wi2",
                        "wi3"
                )

            }
            "w2"{
                "j1"(
                        "wi4",
                        "wi5"

                )
                "j2"(
                        "wi1",
                        "wi2"

                )
            }
        }

        println(current)

    }
}