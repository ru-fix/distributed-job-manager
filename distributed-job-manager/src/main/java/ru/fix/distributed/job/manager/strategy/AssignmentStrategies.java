package ru.fix.distributed.job.manager.strategy;

public class AssignmentStrategies {
    public static final AssignmentStrategy EVENLY_SPREAD = new EvenlySpreadAssignmentStrategy();
    public static final AssignmentStrategy RENDEZVOUS = new RendezvousHashAssignmentStrategy();
    public static final AssignmentStrategy DEFAULT = new EvenlySpreadAssignmentStrategy();
}
