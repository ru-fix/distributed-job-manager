package ru.fix.distributed.job.manager

import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

internal class HearthBeatJob(
    private val jobId: String,
    private val schedule: Schedule,
    workItemsQuantity: Int
) : DistributedJob {

    private val workPool: WorkPool

    val hearthBeatMap: Map<String, AtomicInteger>

    init {
        val workItems: MutableSet<String> = HashSet(workItemsQuantity)
        for (itemNumber in 1..workItemsQuantity) {
            workItems.add("wi-($jobId)-$itemNumber")
        }
        workPool = WorkPool.of(workItems)
        hearthBeatMap = ConcurrentHashMap(workItemsQuantity)
        for (workItemId in workItems) {
            hearthBeatMap[workItemId] = AtomicInteger(0)
        }
    }

    override fun getJobId(): String = jobId

    override fun getSchedule(): DynamicProperty<Schedule> = DynamicProperty.of(schedule)

    @Throws(Exception::class)
    override fun run(context: DistributedJobContext) {
        for (workItemId in context.workShare) {
            hearthBeatMap[workItemId]!!.incrementAndGet()
        }
    }

    override fun getWorkPool(): WorkPool = workPool

    override fun getWorkPoolRunningStrategy(): WorkPoolRunningStrategy =
        WorkPoolRunningStrategies.getSingleThreadStrategy()

    override fun getWorkPoolCheckPeriod(): Long = 0L
}