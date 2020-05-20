package ru.fix.distributed.job.manager.model

import ru.fix.distributed.job.manager.strategy.AssignmentStrategies
import ru.fix.distributed.job.manager.strategy.AssignmentStrategy
import ru.fix.dynamic.property.api.DynamicProperty

data class DistributedJobManagerSettings @JvmOverloads constructor(
        val nodeId: String,
        val rootPath: String,
        val assignmentStrategy: AssignmentStrategy = AssignmentStrategies.DEFAULT,
        /**
         * Time to wait for tasks to be completed when the application is closed and when tasks are redistributed
         * */
        val timeToWaitTermination: DynamicProperty<Long>,
        /**
         * False: djm works normally, all jobs are enabled
         * True: all jobs are disabled
         * */
        val disableAllJobs: DynamicProperty<Boolean> = DynamicProperty.of(false)
)