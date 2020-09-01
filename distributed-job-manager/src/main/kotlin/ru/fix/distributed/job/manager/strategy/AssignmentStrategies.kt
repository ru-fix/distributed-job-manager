package ru.fix.distributed.job.manager.strategy

class AssignmentStrategies {
    companion object {
        @JvmField
        val EVENLY_SPREAD: AssignmentStrategy = EvenlySpreadAssignmentStrategy()
        @JvmField
        val RENDEZVOUS: AssignmentStrategy = RendezvousHashAssignmentStrategy()
        @JvmField
        val DEFAULT: AssignmentStrategy = EvenlyRendezvousAssignmentStrategy()
        @JvmField
        val EVENLY_RENDEZVOUS: AssignmentStrategy = EvenlyRendezvousAssignmentStrategy()
    }
}