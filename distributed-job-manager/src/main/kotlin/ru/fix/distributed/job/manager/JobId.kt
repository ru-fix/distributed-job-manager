package ru.fix.distributed.job.manager

import java.util.*

/**
 * Unique identifies Job type within the cluster.
 */
class JobId(val id: String) {

    private val PATTERN = "[a-zA-Z0-9_-]".toRegex()

    init {
        require(PATTERN.matches(id)) { "JobId '$id' does not match pattern $PATTERN" }
    }

    override fun equals(other: Any?) =
            this === other || id == (other as? JobId)?.id

    override fun hashCode(): Int {
        return Objects.hash(id)
    }

    override fun toString(): String {
        return "JobId[$id]"
    }
}
