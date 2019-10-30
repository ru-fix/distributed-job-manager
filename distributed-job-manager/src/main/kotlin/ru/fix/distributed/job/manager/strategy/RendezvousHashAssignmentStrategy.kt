package ru.fix.distributed.job.manager.strategy

import com.google.common.hash.Funnel
import com.google.common.hash.Hashing
import ru.fix.distributed.job.manager.model.AssignmentState
import ru.fix.distributed.job.manager.model.JobId
import ru.fix.distributed.job.manager.model.WorkItem
import ru.fix.distributed.job.manager.model.WorkerId
import ru.fix.distributed.job.manager.util.RendezvousHash
import java.nio.charset.StandardCharsets
import java.util.*
import kotlin.collections.ArrayList

class RendezvousHashAssignmentStrategy : AbstractAssignmentStrategy() {

    override fun reassignAndBalance(
            availability: MutableMap<JobId, MutableSet<WorkerId>>,
            prevAssignment: AssignmentState,
            currentAssignment: AssignmentState,
            itemsToAssign: MutableSet<WorkItem>
    ): AssignmentState {
        val stringFunnel = Funnel<String> { from, into ->
            into.putBytes(from.toByteArray(StandardCharsets.UTF_8))
        }

        for ((jobId, availableWorkers) in availability) {
            val hash = RendezvousHash<String, String>(
                    Hashing.murmur3_128(), stringFunnel, stringFunnel, ArrayList()
            )
            availableWorkers.forEach { worker ->
                hash.add(worker.id)
                currentAssignment.putIfAbsent(worker, HashSet<WorkItem>())
            }

            val availableItems = getWorkItemsByJob(jobId, itemsToAssign)
            for (item in availableItems) {
                if (currentAssignment.containsWorkItem(item)) {
                    continue
                }

                val workerId = hash.get(item.jobId.id + ":" + item.id)
                currentAssignment.addWorkItem(WorkerId(workerId), item)
                itemsToAssign.remove(item)
            }
        }
        return currentAssignment
    }
}
