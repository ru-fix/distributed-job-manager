package ru.fix.distributed.job.manager.model

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import ru.fix.distributed.job.manager.JobId
import ru.fix.distributed.job.manager.strategy.assignmentState

class AssignmentStateTest {
    @Test
    fun `less busy worker with jobId prefer local pool size over global pool size`() {
        val state = assignmentState {
            "wA"{
                "j0"("i1", "i2")
                "j1"("i1")
            }

            "wB"{
                "j0"("i1", "i2", "i3")
                "j1"("i1")
            }
            "wC"{
                "j0"("i1")
                "j1"("i1", "i2", "i3", "i4", "i5", "i6", "i7")
            }

        }
        state.getLessBusyWorkerWithJobId(JobId("j0"), WorkerId.setOf("wA", "wB", "wC")).shouldBe(WorkerId("wC"))
    }

    @Test
    fun `most busy worker with jobId prefer local pool size over global pool size`() {
        val state = assignmentState {
            "wA"{
                "j0"("i1", "i2", "i3")
                "j1"("i1", "i2")
            }

            "wB"{
                "j0"("i1", "i2", "i3")
                "j1"("i1")
            }
            "wC"{
                "j0"("i1", "i2")
                "j1"("i1", "i2", "i3", "i4", "i5", "i6", "i7")
            }

        }
        state.getMostBusyWorkerWithJobId(JobId("j0"), WorkerId.setOf("wA", "wB", "wC")).shouldBe(WorkerId("wA"))
    }

    @Test
    fun `getLessBusyWorkerWithJobId  WHEN job workPools sizes are different THEN global workPools sizes don't matter`() {

        val state = assignmentState {
            "wA"{
                "j1"("0")
                "j2"("2")
            }

            "wB"{
                "j1"("2")
                "j2"("0")
            }
            "wC"{
                "j1"("1")
                "j2"("0")
            }

        }
        state.getLessBusyWorkerWithJobId(JobId("j1"), WorkerId.setOf("wA", "wB", "wC")).shouldBe(WorkerId("wA"))
    }

    @Test
    fun `lessBusyWorker for empty assignmentState returns worker from availability`() {
        val state = assignmentState { }
        state.getLessBusyWorker(WorkerId.setOf("wA")).shouldBe(WorkerId("wA"))
    }

    @Test
    fun `getLessBusyWorkerWithJobId for empty assignmentState returns worker from availability`() {
        val state = assignmentState { }
        state.getLessBusyWorkerWithJobId(JobId("j1"), WorkerId.setOf("wA")).shouldBe(WorkerId("wA"))
    }

    @Test
    fun `getMostBusyWorkerWithJobId for empty assignmentState returns worker from availability`() {
        val state = assignmentState { }
        state.getMostBusyWorkerWithJobId(JobId("j1"), WorkerId.setOf("wA")).shouldBe(WorkerId("wA"))
    }

    @Test
    fun `mostBusyWorker for empty assignmentState returns worker from availability`() {
        val state = assignmentState { }
        state.getMostBusyWorker(WorkerId.setOf("wA")).shouldBe(WorkerId("wA"))
    }

    @Test
    fun `mostBusyWorker considers worker from availability even if they are absent in assignmetnState`() {
        val state = assignmentState {
            "wA" {
                "j1"("0")
            }
            "wB"{
                "j1"("1", "2")
            }
        }
        state.getMostBusyWorker(WorkerId.setOf("wA", "wB")).shouldBe(WorkerId("wB"))
        state.getMostBusyWorker(WorkerId.setOf("wA", "wB", "wC")).shouldBe(WorkerId("wB"))
    }
}