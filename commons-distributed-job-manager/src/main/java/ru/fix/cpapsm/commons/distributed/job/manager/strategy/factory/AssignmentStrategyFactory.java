package ru.fix.cpapsm.commons.distributed.job.manager.strategy.factory;

import ru.fix.cpapsm.commons.distributed.job.manager.strategy.AssignmentStrategy;

public interface AssignmentStrategyFactory {

    AssignmentStrategy getAssignmentStrategy(String jobId);

}
