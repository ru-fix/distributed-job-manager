package ru.fix.distributed.job.manager.model

import ru.fix.distributed.job.manager.strategy.AssignmentStrategies
import ru.fix.distributed.job.manager.strategy.AssignmentStrategy

data class DistributedJobManagerSettings(
        val nodeId: String,
        val rootPath: String,
        val assignmentStrategy: AssignmentStrategy = AssignmentStrategies.DEFAULT,
        /**
         * Time to wait for tasks to be completed when the application is closed and when tasks are redistributed
         * */
        val jobSettings: DistributedJobsPreset
)