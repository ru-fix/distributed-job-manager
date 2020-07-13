package ru.fix.distributed.job.manager

import org.apache.logging.log4j.kotlin.Logging
import ru.fix.stdlib.concurrency.events.ReducingEventAccumulator
import java.util.concurrent.atomic.AtomicReference

/**
 * Gives information about Manager state (leadership, shutdown status, necessity for rebalance)
 * */
class ManagerState {

    companion object : Logging

    private val currentState = AtomicReference(State.IS_NOT_LEADER)

    private val newRebalanceNeededAccumulator = ReducingEventAccumulator<ManagerEvent, Boolean> { _, newEvent ->
        when (currentState.get()!!) {
            State.IS_NOT_LEADER -> handleRebalanceEventAsNonLeader(newEvent)
            State.IS_LEADER -> handleRebalanceEventAsLeader(newEvent)
            State.SHUTDOWN -> false
        }
    }

    fun publishEvent(event: ManagerEvent) = newRebalanceNeededAccumulator.publishEvent(event)

    fun awaitRebalanceNecessity() = newRebalanceNeededAccumulator.extractAccumulatedValueOrNull() ?: false

    fun isActiveLeader() = currentState.get() == State.IS_LEADER

    fun isClosed() = currentState.get() == State.SHUTDOWN


    /**
     * @return true if new rebalance needed due to received event
     * */
    private fun handleRebalanceEventAsLeader(event: ManagerEvent) = when (event) {
        ManagerEvent.COMMON_REBALANCE_EVENT -> {
            true
        }
        ManagerEvent.LEADERSHIP_LOST -> {
            currentState.set(State.IS_NOT_LEADER)
            false
        }
        ManagerEvent.SHUTDOWN -> {
            currentState.set(State.SHUTDOWN)
            false
        }
        ManagerEvent.LEADERSHIP_ACQUIRED -> {
            logger.warn { "received ${ManagerEvent.LEADERSHIP_ACQUIRED} event, but manager is already the leader" }
            false
        }
    }

    /**
     * @return true if new rebalance needed due to received event
     * */
    private fun handleRebalanceEventAsNonLeader(event: ManagerEvent) = when (event) {
        ManagerEvent.COMMON_REBALANCE_EVENT -> {
            false
        }
        ManagerEvent.LEADERSHIP_ACQUIRED -> {
            currentState.set(State.IS_LEADER)
            true
        }
        ManagerEvent.SHUTDOWN -> {
            currentState.set(State.SHUTDOWN)
            false
        }
        ManagerEvent.LEADERSHIP_LOST -> {
            logger.warn { "received ${ManagerEvent.LEADERSHIP_LOST} event, but manager has already lost leadership" }
            false
        }
    }

    override fun toString(): String {
        return "$currentState"
    }

}

enum class ManagerEvent {
    COMMON_REBALANCE_EVENT, LEADERSHIP_LOST, LEADERSHIP_ACQUIRED, SHUTDOWN
}

private enum class State {
    IS_LEADER, IS_NOT_LEADER, SHUTDOWN
}