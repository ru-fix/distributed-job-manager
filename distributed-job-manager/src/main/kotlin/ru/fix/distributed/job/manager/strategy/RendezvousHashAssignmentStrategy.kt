package ru.fix.distributed.job.manager.strategy

import com.google.common.hash.Funnel
import com.google.common.hash.Hashing
import ru.fix.distributed.job.manager.model.AssignmentState
import ru.fix.distributed.job.manager.model.JobId
import ru.fix.distributed.job.manager.model.WorkItem
import ru.fix.distributed.job.manager.model.WorkerId
import ru.fix.distributed.job.manager.util.RendezvousHash

import java.nio.charset.StandardCharsets
import java.util.ArrayList
import java.util.HashSet

class RendezvousHashAssignmentStrategy : AbstractAssignmentStrategy() {

    override fun reassignAndBalance(
            availability: Map<JobId, Set<WorkerId>>,
            prevAssignment: AssignmentState,
            currentAssignment: AssignmentState,
            itemsToAssign: MutableSet<WorkItem>
    ): AssignmentState {
        val stringFunnel = { from, into -> into.putBytes(from.toByteArray(StandardCharsets.UTF_8)) }

        for ((key, value) in availability) {
            val hash = RendezvousHash<String, String>(
                    Hashing.murmur3_128(), stringFunnel, stringFunnel, ArrayList()
            )
            value.forEach { worker ->
                hash.add(worker.id)
                (currentAssignment as java.util.Map).putIfAbsent(worker, HashSet<WorkItem>())
            }

            val availableItems = getWorkItemsByJob(key, itemsToAssign)
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
