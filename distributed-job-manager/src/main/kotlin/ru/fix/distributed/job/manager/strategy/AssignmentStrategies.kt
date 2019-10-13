package ru.fix.distributed.job.manager.strategy

object AssignmentStrategies {
    val EVENLY_SPREAD: AssignmentStrategy = EvenlySpreadAssignmentStrategy()
    val RENDEZVOUS: AssignmentStrategy = RendezvousHashAssignmentStrategy()
    val DEFAULT: AssignmentStrategy = EvenlySpreadAssignmentStrategy()
}
