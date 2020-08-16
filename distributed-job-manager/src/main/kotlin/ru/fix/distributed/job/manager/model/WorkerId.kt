package ru.fix.distributed.job.manager.model

data class WorkerId(val id: String) : Comparable<WorkerId> {

    override fun compareTo(other: WorkerId): Int {
        return id.compareTo(other.id)
    }

    override fun toString(): String {
        return "Worker[$id]"
    }

    companion object {
        private val PATTERN = "[a-zA-Z0-9_-[.]]+".toRegex()

        fun setOf(vararg ids: String): Set<WorkerId> {
            return ids.map { WorkerId(it) }.toSet()
        }
    }
}
