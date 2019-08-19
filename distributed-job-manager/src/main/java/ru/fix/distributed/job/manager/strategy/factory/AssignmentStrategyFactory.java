package ru.fix.distributed.job.manager.strategy.factory;

import ru.fix.distributed.job.manager.strategy.AssignmentStrategy;

public interface AssignmentStrategyFactory {

    AssignmentStrategy getAssignmentStrategy(String jobId);

}
