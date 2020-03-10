package ru.fix.distributed.job.manager.model

import ru.fix.distributed.job.manager.strategy.AssignmentStrategy
import ru.fix.dynamic.property.api.DynamicProperty

data class DistributedJobManagerSettings(
        val nodeId: String,
        val rootPath: String,
        val assignmentStrategy: AssignmentStrategy,
        val timeToWaitTermination: DynamicProperty<Long>
) {

}