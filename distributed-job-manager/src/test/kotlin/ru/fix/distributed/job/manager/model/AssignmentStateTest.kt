package ru.fix.distributed.job.manager.model

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.*

internal class AssignmentStateTest {

    @Test
    fun `getLessBusyWorkerWithJobId  WHEN job workPools sizes are different THEN global workPools sizes don't matter`() {
        val workerId0 = WorkerId("0")
        val workerId1 = WorkerId("1")
        val workerId2 = WorkerId("2")
        val allWorkers = setOf(workerId0, workerId1, workerId2)

        val jobId1 = JobId("1")
        val jobId2 = JobId("2")

        val state = AssignmentState()
        state.putAll(mapOf(
                worker(workerId0, items(jobId1, 0), items(jobId2, 2)),
                worker(workerId1, items(jobId1, 2), items(jobId2, 0)),
                worker(workerId2, items(jobId1, 1), items(jobId2, 0))
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