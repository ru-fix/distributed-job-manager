package ru.fix.distributed.job.manager.model

import ru.fix.distributed.job.manager.JobId

data class WorkItem(
        val id: String,
        val jobId: JobId
) {
    init {
        require(PATTERN.matches(id)) { "WorkItem '$id' does not match pattern $PATTERN" }
    }

    override fun toString(): String {
        return "WorkItem[job=" + jobId.id + ", id=" + id + "]"
    }

    companion object {
        private val PATTERN = "[a-zA-Z0-9_-[.]]+".toRegex()
    }
}
