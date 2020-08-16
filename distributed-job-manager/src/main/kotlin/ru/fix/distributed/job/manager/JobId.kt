package ru.fix.distributed.job.manager

import java.util.*

/**
 * Unique identifies Job type within the cluster.
 * Job id is a latin string `[a-zA-Z0-9_-]+`
 */
class JobId(val id: String) {
    companion object {
        const val JOB_ID_MAX_LENGTH = 255
        private val PATTERN = "[a-zA-Z0-9._-]+".toRegex()
    }

    init {
        require(id.length <= JOB_ID_MAX_LENGTH) { "JobId '$id' is bigger than $JOB_ID_MAX_LENGTH" }
        require(PATTERN.matches(id)) { "JobId '$id' does not match pattern $PATTERN" }
    }

    override fun equals(other: Any?) =
            this === other || this.id.equals((other as? JobId)?.id)

    override fun hashCode() =
            Objects.hash(id)

    override fun toString() =
            "JobId[$id]"
}
