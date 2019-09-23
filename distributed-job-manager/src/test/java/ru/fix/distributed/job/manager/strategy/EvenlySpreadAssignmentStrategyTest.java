package ru.fix.distributed.job.manager.strategy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.fix.distributed.job.manager.model.WorkItem;
import ru.fix.distributed.job.manager.model.WorkerId;
import ru.fix.distributed.job.manager.model.AssignmentState;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static ru.fix.distributed.job.manager.strategy.AssignmentStrategyUtils.*;

class EvenlySpreadAssignmentStrategyTest {
    private EvenlySpreadAssignmentStrategy evenlySpread;

    @BeforeEach
    public void setUp() {
        evenlySpread = new EvenlySpreadAssignmentStrategy();
    }

    @Test
    public void reassignAndBalanceWhenOnlyOneWorkerHasJobs() {
        AssignmentState available = new AssignmentState();
        AssignmentState previous = new AssignmentState();

        available.addWorkItem(new WorkerId("worker-0"), new WorkItem("work-item-0", "job-0"));
        available.addWorkItem(new WorkerId("worker-0"), new WorkItem("work-item-1", "job-0"));
        available.addWorkItem(new WorkerId("worker-0"), new WorkItem("work-item-2", "job-0"));
        available.addWorkItem(new WorkerId("worker-0"), new WorkItem("work-item-0", "job-1"));
        available.addWorkItem(new WorkerId("worker-0"), new WorkItem("work-item-1", "job-1"));
        available.put(new WorkerId("worker-1"), new HashSet<>());
        available.put(new WorkerId("worker-2"), new HashSet<>());

        previous.put(new WorkerId("worker-0"), new HashSet<>());
        previous.put(new WorkerId("worker-1"), new HashSet<>());
        previous.put(new WorkerId("worker-2"), new HashSet<>());

        assertFalse(available.isBalanced());

        AssignmentState newAssignment = evenlySpread.reassignAndBalance(
                generateAvailability(available),
                previous,
                new AssignmentState()
        );

        assertTrue(newAssignment.isBalanced());
    }

    @Test
    public void reassignAndBalanceWhenSomeWorkersHasJobs() {
        AssignmentState available = new AssignmentState();
        AssignmentState previous = new AssignmentState();

        addWorkerWithItems(available, "worker-0", 3, 3);
        available.addWorkItem(new WorkerId("worker-1"), new WorkItem("work-item-1", "job-3"));
        available.addWorkItem(new WorkerId("worker-1"), new WorkItem("work-item-2", "job-3"));
        available.addWorkItem(new WorkerId("worker-1"), new WorkItem("work-item-0", "job-3"));
        available.put(new WorkerId("worker-2"), new HashSet<>());

        previous.put(new WorkerId("worker-0"), new HashSet<>());
        previous.put(new WorkerId("worker-1"), new HashSet<>());
        previous.put(new WorkerId("worker-2"), new HashSet<>());

        assertFalse(available.isBalanced());

        AssignmentState newAssignment = evenlySpread.reassignAndBalance(
                generateAvailability(available),
                previous,
                new AssignmentState()
        );

        assertTrue(newAssignment.isBalanced());
    }

    @Test
    public void reassignAndBalanceIfWorkerNotAvailable() {
        AssignmentState available = new AssignmentState();
        AssignmentState previous = new AssignmentState();

        addWorkerWithItems(available, "worker-0", 3, 1);

        previous.addWorkItems(new WorkerId("worker-1"), Set.of(
                new WorkItem("work-item-2", "job-3"),
                new WorkItem("work-item-0", "job-3")
        ));
        previous.addWorkItems(new WorkerId("worker-0"), Set.of(
                new WorkItem("work-item-1", "job-3"),
                new WorkItem("work-item-0", "job-0")
        ));

        AssignmentState newAssignment = evenlySpread.reassignAndBalance(
                generateAvailability(available),
                previous,
                new AssignmentState()
        );

        assertTrue(newAssignment.isBalanced());
    }

    @Test
    public void reassignAndBalanceIfNewWorkersAdded() {
        AssignmentState available = new AssignmentState();
        AssignmentState previous = new AssignmentState();

        addWorkerWithItems(available, "worker-0", 3, 1);
        available.addWorkItem(new WorkerId("worker-1"), new WorkItem("work-item-1", "job-3"));
        available.addWorkItem(new WorkerId("worker-1"), new WorkItem("work-item-2", "job-3"));
        available.addWorkItem(new WorkerId("worker-1"), new WorkItem("work-item-0", "job-3"));
        available.addWorkItems(new WorkerId("worker-2"), Collections.emptySet());
        available.addWorkItems(new WorkerId("worker-3"), Collections.emptySet());

        previous.addWorkItems(new WorkerId("worker-0"), Set.of(
                new WorkItem("work-item-1", "job-3"),
                new WorkItem("work-item-0", "job-0")
        ));
        previous.addWorkItems(new WorkerId("worker-1"), Set.of(
                new WorkItem("work-item-0", "job-3"),
                new WorkItem("work-item-3", "job-0")
        ));

        AssignmentState newAssignment = evenlySpread.reassignAndBalance(
                generateAvailability(available),
                previous,
                new AssignmentState()
        );

        assertTrue(newAssignment.isBalanced());
    }

    @Test
    public void reassignAndBalanceIfWorkerNotAvailableAndNewWorkerAdded() {
        AssignmentState available = new AssignmentState();
        AssignmentState previous = new AssignmentState();

        addWorkerWithItems(available, "worker-0", 3, 1);
        available.addWorkItem(new WorkerId("worker-1"), new WorkItem("work-item-1", "job-3"));
        available.addWorkItem(new WorkerId("worker-1"), new WorkItem("work-item-2", "job-3"));
        available.addWorkItem(new WorkerId("worker-1"), new WorkItem("work-item-0", "job-3"));

        // Previous state contains worker-2 instead of worker-1.
        // It's emulate case, when worker-1 is not available, and worker-2 connected
        previous.addWorkItems(new WorkerId("worker-0"), Set.of(
                new WorkItem("work-item-1", "job-3"),
                new WorkItem("work-item-0", "job-0")
        ));
        previous.addWorkItems(new WorkerId("worker-2"), Set.of(
                new WorkItem("work-item-0", "job-3"),
                new WorkItem("work-item-2", "job-0"),
                new WorkItem("work-item-1", "job-0"),
                new WorkItem("work-item-2", "job-3")
        ));

        AssignmentState newAssignment = evenlySpread.reassignAndBalance(
                generateAvailability(available),
                previous,
                new AssignmentState()
        );

        assertTrue(newAssignment.isBalanced());
    }
}