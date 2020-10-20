package ru.fix.distributed.job.manager.strategy

import com.google.common.hash.Funnel
import com.google.common.hash.Hashing
import ru.fix.distributed.job.manager.model.AssignmentState
import ru.fix.distributed.job.manager.model.Availability
import ru.fix.distributed.job.manager.model.WorkItem
import ru.fix.distributed.job.manager.model.WorkerId
import ru.fix.distributed.job.manager.util.RendezvousHash
import java.nio.charset.StandardCharsets
import java.util.*

class EvenlyRendezvousAssignmentStrategy : AbstractAssignmentStrategy() {

    override fun reassignAndBalance(
        availability: Availability,
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

            val itemsCount = itemsToAssignForJob.size
            val workersCount = availableWorkers.size

            val higherLimitItemsOnWorker = higherLimitItemsOnWorker(itemsCount, workersCount)
            val lowerLimitItemsOnWorker = lowerLimitItemsOnWorker(itemsCount, workersCount)

            val expectedCountOfWorkersWithHigherLimitAchieved =
                expectedCountOfWorkersWithHigherLimitAchieved(itemsCount, workersCount)

            var workersCountHigherLimitAchieved = 0
            val excludedWorkerIds = mutableSetOf<String>()

            for (item in itemsToAssignForJob) {
                val key = item.jobId.id + ":" + item.id
                val workerId = hash.get(key, excludedWorkerIds)

                currentAssignment.addWorkItem(WorkerId(workerId), item)
                itemsToAssign.remove(item)

                val itemsOnWorkerAfterAddition = currentAssignment.localPoolSize(jobId, WorkerId(workerId))

                if (workersCountHigherLimitAchieved < expectedCountOfWorkersWithHigherLimitAchieved) {
                    if (itemsOnWorkerAfterAddition == higherLimitItemsOnWorker) {
                        workersCountHigherLimitAchieved++
                        excludedWorkerIds.add(workerId)

                        if (workersCountHigherLimitAchieved == expectedCountOfWorkersWithHigherLimitAchieved) {
                            availableWorkers
                                .filter { !excludedWorkerIds.contains(it.id) }
                                .filter { currentAssignment.localPoolSize(jobId, it) == lowerLimitItemsOnWorker }
                                .forEach { excludedWorkerIds.add(it.id) }
                        }

                    }
                } else {
                    if (itemsOnWorkerAfterAddition == lowerLimitItemsOnWorker) {
                        excludedWorkerIds.add(workerId)
                    }
                }
            }
        }
    }

    private fun higherLimitItemsOnWorker(itemsCount: Int, workersCount: Int): Int {
        return if (itemsCount % workersCount == 0)
            itemsCount / workersCount
        else
            itemsCount / workersCount + 1
    }

    private fun lowerLimitItemsOnWorker(itemsCount: Int, workersCount: Int): Int {
        return if (itemsCount % workersCount == 0)
            itemsCount / workersCount
        else
            itemsCount / workersCount
    }

    private fun expectedCountOfWorkersWithHigherLimitAchieved(itemsCount: Int, workersCount: Int): Int {
        return if (itemsCount % workersCount == 0) workersCount else itemsCount % workersCount
    }
}
