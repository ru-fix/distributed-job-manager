package ru.fix.distributed.job.manager.model

import ru.fix.distributed.job.manager.strategy.AssignmentStrategies
import ru.fix.distributed.job.manager.strategy.AssignmentStrategy
import ru.fix.dynamic.property.api.DynamicProperty

data class DistributedJobManagerSettings(
        val nodeId: String,
        val rootPath: String,
        val assignmentStrategy: AssignmentStrategy = AssignmentStrategies.DEFAULT,
        /**
         * Time to wait for tasks to be completed when the application is closed and when tasks are redistributed
         * */
        val timeToWaitTermination: DynamicProperty<Long>,
        /**
         * Delay between launching task for removing not relevant jobs from `work-pool` subtree.
         * Minor process. Default value is one hour
         * */
        val workPoolCleanPeriodMs: DynamicProperty<Long> = DynamicProperty.of(3_600_000)
)