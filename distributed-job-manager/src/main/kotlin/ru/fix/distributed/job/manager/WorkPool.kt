package ru.fix.distributed.job.manager

/**
 * [WorkPool] represent set of items to process within the cluster by [DistributedJob] instances with same [JobId].
 * Items will be distributed between [DistributedJob] instances with same [JobId] based on [ru.fix.distributed.job.manager.strategy.AssignmentStrategy]
 */
class WorkPool(items: Set<String>) {

    companion object {
        const val WORK_POOL_ITEM_MAX_LENGTH = 255

        @JvmStatic
        fun of(items: Set<String>): WorkPool {
            return WorkPool(items)
        }

        @JvmStatic
        fun singleton(): WorkPool {
            return WorkPool(setOf("singleton"))
        }
    }

    val items: Set<String> = HashSet(items)
}