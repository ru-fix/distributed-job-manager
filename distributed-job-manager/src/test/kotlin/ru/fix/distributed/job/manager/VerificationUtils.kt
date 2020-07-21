package ru.fix.distributed.job.manager

import org.awaitility.Awaitility
import org.hamcrest.MatcherAssert.assertThat
import java.util.concurrent.TimeUnit


/**
 * Checks:
 * * local workPools of jobs in total give common work-pool
 * * local workPools of jobs sizes differs less than two
 *
 * @param jobInstancesOnWorkers instances of single job from every worker in cluster,
 * which can proceed *only* that job.
 */
internal fun verifySingleJobIsDistributedBetweenWorkers(
        durationSec: Long, vararg jobInstancesOnWorkers: StubbedMultiJob) {
    require(jobsHasSameIdAndSameWorkPool(*jobInstancesOnWorkers)) {
        "This method can verify workPool distribution only if workers have same single job." +
                "given stubbed jobs: " + jobInstancesOnWorkers.contentToString()
    }
    val commonWorkPool = jobInstancesOnWorkers.first().getWorkPool().items
    Awaitility.await().atMost(durationSec, TimeUnit.SECONDS).untilAsserted {

        val localWorkPools = jobInstancesOnWorkers.map { it.localWorkPool }

        assertThat("work-pool from locals isn't equal common work-pool." +
                " localWorkPools=$localWorkPools commonWorkPool=$commonWorkPool",
                collectionsContainSameElements(localWorkPools.flatten(), commonWorkPool)
        )
        assertThat("work-pool isn't distributed evenly. localWorkPools=$localWorkPools",
                setsSizesDifferLessThanTwo(localWorkPools)
        )
    }
}

fun collectionsContainSameElements(c1: Collection<String>, c2: Collection<String>): Boolean {
    return c1.size == c2.size && c1.containsAll(c2)
}

private fun jobsHasSameIdAndSameWorkPool(vararg jobs: DistributedJob): Boolean {
    val firstJob = jobs[0]
    val id = firstJob.getJobId().id
    val workPool = firstJob.getWorkPool().items
    for (nextJob in jobs) {
        val nextId = nextJob.getJobId().id
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
