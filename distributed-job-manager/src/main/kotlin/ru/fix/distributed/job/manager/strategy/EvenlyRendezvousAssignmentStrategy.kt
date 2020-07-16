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
    ) {
        val stringFunnel = Funnel<String> { from, into ->
            into.putBytes(from.toByteArray(StandardCharsets.UTF_8))
        }

        for ((jobId, availableWorkers) in availability) {
            val itemsToAssignForJob = getWorkItemsByJob(jobId, itemsToAssign).sortedBy { it.id }
            val hash = RendezvousHash<String, String>(
                    Hashing.murmur3_128(), stringFunnel, stringFunnel, ArrayList()
            )
            availableWorkers.forEach { worker ->
                hash.add(worker.id)
                currentAssignment.putIfAbsent(worker, HashSet<WorkItem>())
            }

            val workersCount = availableWorkers.size
            val workItemsCount = itemsToAssignForJob.size
            var limitWorkItemsOnWorker = limitWorkItemsOnWorker(workItemsCount, workersCount)
            val expectedWorkersNumberWithHigherLimitAchieved = majorityLimit(workItemsCount, workersCount)
            var workersCountHigherLimitAchieved = 0
            val excludedWorkerIds = mutableSetOf<String>()
            var limitDecreased = false

            for (item in itemsToAssignForJob) {
                val key = item.jobId.id + ":" + item.id
                val workerId = hash.get(key, excludedWorkerIds)

                currentAssignment.addWorkItem(WorkerId(workerId), item)
                itemsToAssign.remove(item)

                val workPoolSizeAfterAdd = currentAssignment.localPoolSize(jobId, WorkerId(workerId))
                if (workPoolSizeAfterAdd == limitWorkItemsOnWorker) {
                    if (expectedWorkersNumberWithHigherLimitAchieved != workersCountHigherLimitAchieved || limitDecreased) {
                        workersCountHigherLimitAchieved++
                        excludedWorkerIds.add(workerId)
                    }
                }
                if (expectedWorkersNumberWithHigherLimitAchieved == workersCountHigherLimitAchieved && !limitDecreased) {
                    limitDecreased = true
                    limitWorkItemsOnWorker--
                    availableWorkers
                            .filter { currentAssignment.localPoolSize(jobId, it) == limitWorkItemsOnWorker }
                            .forEach { excludedWorkerIds.add(it.id) }
                }
            }
        }
    }

    private fun limitWorkItemsOnWorker(workItemsCount: Int, workersCount: Int): Int {
        return workItemsCount / workersCount + if (workItemsCount % workersCount == 0) 0 else 1
    }

    private fun majorityLimit(workItemsCount: Int, workersCount: Int): Int {
        return if (workItemsCount % workersCount == 0) workersCount else workItemsCount % workersCount
    }
}
