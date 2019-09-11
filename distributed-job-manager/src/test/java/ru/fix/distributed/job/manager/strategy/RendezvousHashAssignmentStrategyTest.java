package ru.fix.distributed.job.manager.strategy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.fix.distributed.job.manager.model.WorkItem;
import ru.fix.distributed.job.manager.model.WorkerItem;
import ru.fix.distributed.job.manager.model.ZookeeperState;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static ru.fix.distributed.job.manager.strategy.AssignmentStrategyUtils.*;

class RendezvousHashAssignmentStrategyTest {
    private RendezvousHashAssignmentStrategy rendezvous;

    @BeforeEach
    void setUp() {
        rendezvous = new RendezvousHashAssignmentStrategy();
    }

    @Test
    public void reassignAndBalanceWhenOnlyOneWorkerHasJobs() {
        ZookeeperState available = new ZookeeperState();
        ZookeeperState previous = new ZookeeperState();

        available.addWorkItem(new WorkerItem("worker-0"), new WorkItem("work-item-0", "job-0"));
        available.addWorkItem(new WorkerItem("worker-0"), new WorkItem("work-item-1", "job-0"));
        available.addWorkItem(new WorkerItem("worker-0"), new WorkItem("work-item-2", "job-0"));
        available.addWorkItem(new WorkerItem("worker-0"), new WorkItem("work-item-0", "job-1"));
        available.addWorkItem(new WorkerItem("worker-0"), new WorkItem("work-item-1", "job-1"));
        available.put(new WorkerItem("worker-1"), Collections.emptyList());
        available.put(new WorkerItem("worker-2"), Collections.emptyList());

        previous.put(new WorkerItem("worker-0"), Collections.emptyList());
        previous.put(new WorkerItem("worker-1"), Collections.emptyList());
        previous.put(new WorkerItem("worker-2"), Collections.emptyList());

        ZookeeperState currentState = generateCurrentState(available, previous);

        assertFalse(available.isBalanced());
        System.err.println(available);
        System.err.println(previous);
        System.err.println(currentState);
        System.err.println(generateItemsToAssign(available, currentState));
        ZookeeperState newAssignment = rendezvous.reassignAndBalance(
                available,
                previous,
                currentState,
                generateItemsToAssign(available, currentState)
        );
        System.err.println(newAssignment);

        assertEquals(available.globalPoolSize(), newAssignment.globalPoolSize());
    }

    @Test
    public void reassignAndBalanceWhenSomeWorkersHasJobs() {
        ZookeeperState available = new ZookeeperState();
        ZookeeperState previous = new ZookeeperState();

        addWorkerWithItems(available, "worker-0", 3, 3);
        available.addWorkItem(new WorkerItem("worker-1"), new WorkItem("work-item-1", "job-3"));
        available.addWorkItem(new WorkerItem("worker-1"), new WorkItem("work-item-2", "job-3"));
        available.addWorkItem(new WorkerItem("worker-1"), new WorkItem("work-item-0", "job-3"));
        available.put(new WorkerItem("worker-2"), Collections.emptyList());

        previous.put(new WorkerItem("worker-0"), Collections.emptyList());
        previous.put(new WorkerItem("worker-1"), Collections.emptyList());
        previous.put(new WorkerItem("worker-2"), Collections.emptyList());

        ZookeeperState currentState = generateCurrentState(available, previous);

        assertFalse(available.isBalanced());

        System.err.println(available);
        System.err.println(previous);
        System.err.println(currentState);

        ZookeeperState newAssignment = rendezvous.reassignAndBalance(
                available,
                previous,
                currentState,
                generateItemsToAssign(available, currentState)
        );
        System.err.println(newAssignment);

        assertEquals(available.globalPoolSize(), newAssignment.globalPoolSize());
    }

    @Test
    public void reassignAndBalanceIfWorkerNotAvailable() {
        ZookeeperState available = new ZookeeperState();
        ZookeeperState previous = new ZookeeperState();

        addWorkerWithItems(available, "worker-0", 3, 1);

        previous.addWorkItems(new WorkerItem("worker-1"), Arrays.asList(
                new WorkItem("work-item-2", "job-3"),
                new WorkItem("work-item-0", "job-3")
        ));
        previous.addWorkItems(new WorkerItem("worker-0"), Arrays.asList(
                new WorkItem("work-item-1", "job-3"),
                new WorkItem("work-item-0", "job-0")
        ));

        ZookeeperState currentState = generateCurrentState(available, previous);

        System.err.println(available);
        System.err.println(previous);
        System.err.println(currentState);
        System.err.println(generateItemsToAssign(available, currentState));
        ZookeeperState newAssignment = rendezvous.reassignAndBalance(
                available,
                previous,
                currentState,
                generateItemsToAssign(available, currentState)
        );
        System.err.println(newAssignment);

        // 4, because 3 work item from available and 1 from current state
        assertEquals(4, newAssignment.globalPoolSize());
    }

    @Test
    public void reassignAndBalanceIfNewWorkersAdded() {
        ZookeeperState available = new ZookeeperState();
        ZookeeperState previous = new ZookeeperState();

        addWorkerWithItems(available, "worker-0", 3, 1);
        available.addWorkItem(new WorkerItem("worker-1"), new WorkItem("work-item-1", "job-3"));
        available.addWorkItem(new WorkerItem("worker-1"), new WorkItem("work-item-2", "job-3"));
        available.addWorkItem(new WorkerItem("worker-1"), new WorkItem("work-item-0", "job-3"));
        available.addWorkItems(new WorkerItem("worker-2"), Collections.emptyList());
        available.addWorkItems(new WorkerItem("worker-3"), Collections.emptyList());

        previous.addWorkItems(new WorkerItem("worker-0"), Arrays.asList(
                new WorkItem("work-item-1", "job-3"),
                new WorkItem("work-item-0", "job-0")
        ));
        previous.addWorkItems(new WorkerItem("worker-1"), Arrays.asList(
                new WorkItem("work-item-0", "job-3")
        ));

        ZookeeperState currentState = generateCurrentState(available, previous);

        assertFalse(currentState.isBalanced());

        System.err.println(available);
        System.err.println(previous);
        System.err.println(currentState);

        ZookeeperState newAssignment = rendezvous.reassignAndBalance(
                available,
                previous,
                currentState,
                generateItemsToAssign(available, currentState)
        );
        System.err.println(newAssignment);

        assertEquals(available.globalPoolSize(), newAssignment.globalPoolSize());
    }

    @Test
    public void reassignAndBalanceIfWorkerNotAvailableAndNewWorkerAdded() {
        ZookeeperState available = new ZookeeperState();
        ZookeeperState previous = new ZookeeperState();

        addWorkerWithItems(available, "worker-0", 3, 1);
        available.addWorkItem(new WorkerItem("worker-1"), new WorkItem("work-item-1", "job-3"));
        available.addWorkItem(new WorkerItem("worker-1"), new WorkItem("work-item-2", "job-3"));
        available.addWorkItem(new WorkerItem("worker-1"), new WorkItem("work-item-0", "job-3"));

        // Previous state contains worker-2 instead of worker-1.
        // It's emulate case, when worker-1 is not available, and worker-2 connected
        previous.addWorkItems(new WorkerItem("worker-0"), Arrays.asList(
                new WorkItem("work-item-1", "job-3"),
                new WorkItem("work-item-0", "job-0")
        ));
        previous.addWorkItems(new WorkerItem("worker-2"), Arrays.asList(
                new WorkItem("work-item-0", "job-3"),
                new WorkItem("work-item-2", "job-0"),
                new WorkItem("work-item-1", "job-0"),
                new WorkItem("work-item-2", "job-3")
        ));

        ZookeeperState currentState = generateCurrentState(available, previous);

        assertFalse(currentState.isBalanced());
        System.err.println(available);
        System.err.println(previous);
        System.err.println(currentState);
        System.err.println(generateItemsToAssign(available, currentState));
        ZookeeperState newAssignment = rendezvous.reassignAndBalance(
                available,
                previous,
                currentState,
                generateItemsToAssign(available, currentState)
        );
        System.err.println(newAssignment);
        assertEquals(previous.globalPoolSize(), newAssignment.globalPoolSize());
    }
}