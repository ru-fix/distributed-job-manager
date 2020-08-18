package ru.fix.distributed.job.manager

import org.awaitility.Awaitility
import org.hamcrest.MatcherAssert
import org.slf4j.LoggerFactory
import ru.fix.distributed.job.manager.model.JobIdResolver.resolveJobId
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.Schedule
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
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

    val allWorkItems: MutableSet<String> = Collections.newSetFromMap(ConcurrentHashMap<String, Boolean>())

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
        allWorkItems.addAll(context.workShare)
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

/**
 * await until [assertSingleJobIsDistributedBetweenWorkers] asserted
 * */
fun awaitSingleJobIsDistributedBetweenWorkers(
        durationSec: Long, vararg jobInstancesOnWorkers: StubbedMultiJob) {
    Awaitility.await().atMost(durationSec, TimeUnit.SECONDS).untilAsserted {
        assertSingleJobIsDistributedBetweenWorkers(*jobInstancesOnWorkers)
    }
}

/**
 * Checks:
 * * local workPools of jobs in total give common work-pool
 * * local workPools of jobs sizes differs less than two
 *
 * @param jobInstancesOnWorkers instances of single job from every worker in cluster,
 * which can proceed *only* that job.
 */
fun assertSingleJobIsDistributedBetweenWorkers(vararg jobInstancesOnWorkers: StubbedMultiJob) {
    require(jobsHasSameIdAndSameWorkPool(*jobInstancesOnWorkers)) {
        "This method can verify workPool distribution only if workers have same single job." +
                "given stubbed jobs: " + jobInstancesOnWorkers.contentToString()
    }

    val commonWorkPool = jobInstancesOnWorkers.first().getWorkPool().items

    val localWorkPools = jobInstancesOnWorkers.map { it.localWorkPool }

    MatcherAssert.assertThat("work-pool from locals isn't equal common work-pool." +
            " localWorkPools=$localWorkPools commonWorkPool=$commonWorkPool",
            collectionsContainSameElements(localWorkPools.flatten(), commonWorkPool)
    )
    MatcherAssert.assertThat("work-pool isn't distributed evenly. localWorkPools=$localWorkPools",
            setsSizesDifferLessThanTwo(localWorkPools)
    )
}

private fun collectionsContainSameElements(c1: Collection<String>, c2: Set<String>): Boolean {
    return c1.size == c2.size && c1.containsAll(c2)
}

private fun jobsHasSameIdAndSameWorkPool(vararg jobs: DistributedJob): Boolean {
    val firstJob = jobs[0]
    val id = resolveJobId(firstJob)
    val workPool = firstJob.getWorkPool().items
    for (nextJob in jobs) {
        val nextId = resolveJobId(nextJob)
        val nextWorkPool = nextJob.getWorkPool().items
        if (nextId != id || nextWorkPool != workPool) {
            return false
        }
    }
    return true
}

private fun setsSizesDifferLessThanTwo(sets: List<Set<String>>): Boolean {
    val firstSet = sets[0]
    var maxSize = firstSet.size
    var minSize = maxSize
    for (set in sets) {
        val size = set.size
        maxSize = Integer.max(maxSize, size)
        minSize = Integer.min(minSize, size)
        if (maxSize - minSize > 2) {
            return false
        }
    }
    return true
}
