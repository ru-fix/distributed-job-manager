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

class EvenlyRendezvousAssignmentStrategy : AbstractAssignmentStrategy() {

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
            val itemsToAssignForJob = getWorkItemsByJob(jobId, itemsToAssign).sortedBy { it.id }

            val workersCount = availableWorkers.size
            val workItemsCount = itemsToAssignForJob.size
            var limitWorkItemsOnWorker = workItemsCount / workersCount + if (workItemsCount % workersCount == 0) 0 else 1
            val majorityLimit = if (workItemsCount % workersCount == 0) workersCount else workItemsCount % workersCount
            var workersCountLimitAchieved = 0

            val hash = RendezvousHash<String, String>(
                    Hashing.murmur3_128(), stringFunnel, stringFunnel, ArrayList()
            )
            availableWorkers.forEach { worker ->
                hash.add(worker.id)
                currentAssignment.putIfAbsent(worker, HashSet<WorkItem>())
            }
            val fullWorkerIds = mutableSetOf<String>()
            var limitDecreased = false
            for (item in itemsToAssignForJob) {
                val key = item.jobId.id + ":" + item.id
                var workerId = hash.get(key)
                val willBe = currentAssignment.localPoolSize(jobId, WorkerId(workerId)) + 1

                if (willBe > limitWorkItemsOnWorker) {
                    workerId = hash.get(key, fullWorkerIds)
                }
                currentAssignment.addWorkItem(WorkerId(workerId), item)
                itemsToAssign.remove(item)

                val workPoolSizeAfterAdd = currentAssignment.localPoolSize(jobId, WorkerId(workerId))
                if (workPoolSizeAfterAdd == limitWorkItemsOnWorker
                        && (majorityLimit != workersCountLimitAchieved || limitDecreased)
                ) {
                    workersCountLimitAchieved++
                    fullWorkerIds.add(workerId)
                }
                if (majorityLimit == workersCountLimitAchieved && !limitDecreased) {
                    limitDecreased = true
                    limitWorkItemsOnWorker--
                    availableWorkers.forEach {
                        if (currentAssignment.localPoolSize(jobId, it) == limitWorkItemsOnWorker) {
                            fullWorkerIds.add(it.id)
                        }
                    }
                }

            }

        }
        return currentAssignment
    }
}
