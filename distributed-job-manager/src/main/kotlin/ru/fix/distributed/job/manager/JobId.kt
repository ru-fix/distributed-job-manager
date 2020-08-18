package ru.fix.distributed.job.manager

import java.util.*

/**
 * Unique identifies Job type within the cluster.
 * Job id is a latin string `[a-zA-Z0-9._-]+`
 */
class JobId(val id: String) {
    init {
        IdentityValidator.validate(IdentityValidator.IdentityType.JobId, id)
    }

    override fun equals(other: Any?) =
            this === other || this.id == (other as? JobId)?.id

    override fun hashCode() = Objects.hash(id)

    override fun toString() = "JobId[$id]"
}
