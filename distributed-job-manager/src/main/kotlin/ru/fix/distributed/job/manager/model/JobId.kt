package ru.fix.distributed.job.manager.model

import java.util.*

class JobId(
        val id: String
) {

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as JobId

        return id == other.id
    }

    override fun hashCode(): Int {
        return Objects.hash(id)
    }

    override fun toString(): String {
        return "Job[$id]"
    }
}
