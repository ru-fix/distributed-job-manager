package ru.fix.distributed.job.manager

import java.util.*

/**
 * Unique identifies Job type within the cluster.
 */
class JobId(val id: String) {
    companion object {
        private val PATTERN = "[a-zA-Z0-9_-]+".toRegex()
    }

    init {
        require(PATTERN.matches(id)) { "JobId '$id' does not match pattern $PATTERN" }
    }

    override fun equals(other: Any?) =
            this === other || this.id.equals((other as? JobId)?.id)

    override fun hashCode() =
            Objects.hash(id)

    override fun toString() =
            "JobId[$id]"
}
