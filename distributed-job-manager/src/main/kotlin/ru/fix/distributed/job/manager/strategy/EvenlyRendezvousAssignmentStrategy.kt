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
            var higherLimitWorkItemsOnWorker = workItemsCount / workersCount + if (workItemsCount % workersCount == 0) 0 else 1
            var lowerLimitWorkItemsOnWorker = workItemsCount / workersCount
            val expectedNumOfWorkersWithHigherLimitAchieved = if (workItemsCount % workersCount == 0) workersCount else workItemsCount % workersCount
            var workersCountHigherLimitAchieved = 0

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
                var workerId = hash.get(key, fullWorkerIds)

                currentAssignment.addWorkItem(WorkerId(workerId), item)
                itemsToAssign.remove(item)

                val workPoolSizeAfterAdd = currentAssignment.localPoolSize(jobId, WorkerId(workerId))
                if (workPoolSizeAfterAdd == higherLimitWorkItemsOnWorker) {
                    fullWorkerIds.add(workerId)
                    workersCountHigherLimitAchieved++

                    if (workersCountHigherLimitAchieved == expectedNumOfWorkersWithHigherLimitAchieved) {
                        if (!limitDecreased) {
                            limitDecreased = true
                            availableWorkers.forEach {
                                if (currentAssignment.localPoolSize(jobId, it) == lowerLimitWorkItemsOnWorker) {
                                    fullWorkerIds.add(it.id)
                                }
                            }
                        }
                    }
                } else if (limitDecreased && workPoolSizeAfterAdd == lowerLimitWorkItemsOnWorker) {
                    fullWorkerIds.add(workerId)

                }
            }

        }
        return currentAssignment
    }
}
