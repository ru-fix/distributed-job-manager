package ru.fix.distributed.job.manager.strategy.factory;

import ru.fix.distributed.job.manager.strategy.AssignmentStrategy;
import ru.fix.distributed.job.manager.strategy.DefaultAssignmentStrategy;

public class DefaultAssignmentStrategyFactory implements AssignmentStrategyFactory {

    private final DefaultAssignmentStrategy defaultAssignmentStrategy = new DefaultAssignmentStrategy();

    @Override
    public AssignmentStrategy getAssignmentStrategy(String jobId) {
        return defaultAssignmentStrategy;
    }

}
