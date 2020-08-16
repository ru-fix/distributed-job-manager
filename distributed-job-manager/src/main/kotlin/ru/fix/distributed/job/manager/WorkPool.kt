package ru.fix.distributed.job.manager

/**
 * [WorkPool] represent set of items to process within the cluster by [DistributedJob] instances with same [JobId].
 * Items will be distributed between [DistributedJob] instances with same [JobId]
 * based on [ru.fix.distributed.job.manager.strategy.AssignmentStrategy]
 *
 * WorkPool item should be a latin string [a-zA-Z0-9_.-] no more that [WorkPool.WORK_POOL_ITEM_MAX_LENGTH] size
 */
class WorkPool(items: Set<String>) {

    companion object {
        const val WORK_POOL_ITEM_MAX_LENGTH = 255
        private val PATTERN = "[a-zA-Z0-9._-]+".toRegex()

        @JvmStatic
        fun of(items: Set<String>): WorkPool {
            return WorkPool(items)
        }

        @JvmStatic
        fun of(vararg items: String): WorkPool {
            return WorkPool(items.toSet())
        }

        @JvmStatic
        fun singleton(): WorkPool {
            return WorkPool(setOf("singleton"))
        }
    }

    init {
        for (item in items) {
            require(item.length <= WORK_POOL_ITEM_MAX_LENGTH) {
                "Item '$item' is bigger than $WORK_POOL_ITEM_MAX_LENGTH"
            }
            require(PATTERN.matches(item)) { "Item '$item' does not match pattern $PATTERN" }
        }
    }

    val items: Set<String> = HashSet(items)
}