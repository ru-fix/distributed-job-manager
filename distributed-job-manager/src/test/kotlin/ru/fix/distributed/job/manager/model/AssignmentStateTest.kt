package ru.fix.distributed.job.manager.model

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.*

internal class AssignmentStateTest {

    @Test
    fun `getLessBusyWorkerWithJobId localWorkPool is more important than globalWorkPool`() {
        val workerId0 = WorkerId("0")
        val workerId1 = WorkerId("1")
        val workerId2 = WorkerId("2")
        val workerId3 = WorkerId("3")
        val workerId4 = WorkerId("4")
        val allWorkers = setOf(workerId0, workerId1, workerId2, workerId3, workerId4)

        val jobId1 = JobId("1")
        val jobId2 = JobId("2")

        val state = AssignmentState()
        state.putAll(mapOf(
                worker(workerId0, items(jobId1, 56), items(jobId2, 6)),
                worker(workerId1, items(jobId1, 58), items(jobId2, 4)),
                worker(workerId2, items(jobId1, 57), items(jobId2, 4)),
                worker(workerId3, items(jobId1, 57), items(jobId2, 4)),
                worker(workerId4, items(jobId1, 57), items(jobId2, 5))
        ))

        assertEquals(workerId0, state.getLessBusyWorkerWithJobId(jobId1, allWorkers))
    }


    private fun worker(workerId: WorkerId, vararg jobs: HashSet<WorkItem>): Pair<WorkerId, HashSet<WorkItem>> {
        val items = hashSetOf<WorkItem>()
        jobs.forEach { items.addAll(it) }
        return workerId to items
    }

    private fun items(jobId: JobId, count: Int): HashSet<WorkItem> {
        val items = hashSetOf<WorkItem>()
        repeat(count) {
            items.add(WorkItem(UUID.randomUUID().toString(), jobId))
        }
        return items
    }
}