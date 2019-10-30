package ru.fix.distributed.job.manager.strategy

class AssignmentStrategies {
    companion object {
        val EVENLY_SPREAD: AssignmentStrategy = EvenlySpreadAssignmentStrategy()
        val RENDEZVOUS: AssignmentStrategy = RendezvousHashAssignmentStrategy()
        val DEFAULT: AssignmentStrategy = EvenlyRendezvousAssignmentStrategy()
        val EVENLY_RENDEZVOUS: AssignmentStrategy = EvenlyRendezvousAssignmentStrategy()
    }
}