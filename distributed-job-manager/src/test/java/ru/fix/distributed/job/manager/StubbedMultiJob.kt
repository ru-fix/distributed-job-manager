package ru.fix.distributed.job.manager

import org.slf4j.LoggerFactory
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule
import java.util.*
import java.util.concurrent.atomic.AtomicReference

open class StubbedMultiJob @JvmOverloads constructor(
        jobId: Int,
        private var workPool: Set<String>,
        private val delay: Long = 100,
        private val workPoolExpirationPeriod: Long = 0,
        private val singleThread: Boolean = true
) : DistributedJob {

    private val localWorkPoolReference = AtomicReference<Set<String>>()
    val localWorkPool: Set<String>
        get() = localWorkPoolReference.get() ?: emptySet()

    val allWorkPools: MutableSet<Set<String>> = Collections.synchronizedSet(HashSet<Set<String>>())

    fun updateWorkPool(newWorkPool: Set<String>) {
        workPool = newWorkPool
    }

    override val jobId = getJobId(jobId)

    override fun getWorkPool(): WorkPool = WorkPool.of(workPool)

    override fun getWorkPoolRunningStrategy(): WorkPoolRunningStrategy {
        return if (singleThread)
            WorkPoolRunningStrategies.getSingleThreadStrategy()
        else
            WorkPoolRunningStrategies.getThreadPerWorkItemStrategy()
    }

    override fun getInitialJobDelay(): DynamicProperty<Long> = DynamicProperty.of(0L)

    override fun getSchedule(): DynamicProperty<Schedule> = Schedule.withDelay(DynamicProperty.of(delay))

    override fun getWorkPoolCheckPeriod(): Long = workPoolExpirationPeriod

    override fun run(context: DistributedJobContext) {
        logger.trace("{} Run distributed test job {} / {}", this, jobId, context.workShare)
        localWorkPoolReference.set(context.workShare)
        allWorkPools.add(context.workShare)
    }


    companion object {
        private val logger = LoggerFactory.getLogger(StubbedMultiJob::class.java)
        private const val DISTRIBUTED_JOB_ID_PATTERN = "distr-job-id-%d"

        @JvmStatic
        fun getJobId(id: Int): JobId {
            return JobId(String.format(DISTRIBUTED_JOB_ID_PATTERN, id))
        }
    }

}