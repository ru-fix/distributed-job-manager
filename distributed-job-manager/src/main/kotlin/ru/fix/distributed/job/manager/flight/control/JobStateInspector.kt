package ru.fix.distributed.job.manager.flight.control

interface JobStateInspector {
    fun getJobStatus(jobId: String): JobStatus
}

enum class JobStatus {
    STARTED,
    STOPPED
}


