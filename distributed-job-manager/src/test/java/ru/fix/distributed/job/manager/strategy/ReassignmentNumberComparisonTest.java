package ru.fix.distributed.job.manager.strategy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.fix.distributed.job.manager.model.AssignmentState;
import ru.fix.distributed.job.manager.model.JobId;
import ru.fix.distributed.job.manager.model.WorkItem;
import ru.fix.distributed.job.manager.model.WorkerId;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static ru.fix.distributed.job.manager.strategy.AssignmentStrategyUtils.*;

class ReassignmentNumberComparisonTest {
    private EvenlySpreadAssignmentStrategy evenlySpread;
    private RendezvousHashAssignmentStrategy rendezvous;

    @BeforeEach
    void setUp() {
        evenlySpread = new EvenlySpreadAssignmentStrategy();
        rendezvous = new RendezvousHashAssignmentStrategy();
    }

    private static class Results {
        final int evenlySpreadReassignmentNumber;
        final int rendezvousReassignmentNumber;

        private Results(int evenlySpreadReassignmentNumber, int rendezvousReassignmentNumber) {
            this.evenlySpreadReassignmentNumber = evenlySpreadReassignmentNumber;
            this.rendezvousReassignmentNumber = rendezvousReassignmentNumber;
        }
    }

    @Test
    void balanceItemsOfSingleJobBetweenTwoWorkers() {
        AssignmentState available = new AssignmentState();
        AssignmentState previous = new AssignmentState();

        Set<WorkItem> workItems = generateWorkItems(new JobId("job-0"), 0, 4);
        available.addWorkItems(new WorkerId("worker-0"), workItems);
        available.addWorkItems(new WorkerId("worker-1"), workItems);

        previous.addWorkItems(new WorkerId("worker-0"), Set.of(
                new WorkItem("work-item-0", new JobId("job-0")),
                new WorkItem("work-item-1", new JobId("job-0")),
                new WorkItem("work-item-2", new JobId("job-0"))
        ));
        previous.addWorkItems(new WorkerId("worker-1"), Set.of(
                new WorkItem("work-item-3", new JobId("job-0")))
        );

        Results results = reassignmentResults(available, previous);
        assertEquals(1, results.evenlySpreadReassignmentNumber);
        assertEquals(2, results.rendezvousReassignmentNumber);
    }

    @Test
    void balanceItemsOfSingleJobBetweenThreeWorkers() {
        AssignmentState available = new AssignmentState();
        AssignmentState previous = new AssignmentState();

        Set<WorkItem> workItems = generateWorkItems(new JobId("job-0"), 0, 4);
        available.addWorkItems(new WorkerId("worker-0"), workItems);
        available.addWorkItems(new WorkerId("worker-1"), workItems);
        available.addWorkItems(new WorkerId("worker-2"), workItems);

        previous.addWorkItems(new WorkerId("worker-0"), Set.of(
                new WorkItem("work-item-0", new JobId("job-0")),
                new WorkItem("work-item-1", new JobId("job-0")),
                new WorkItem("work-item-2", new JobId("job-0"))
        ));
        previous.addWorkItems(new WorkerId("worker-1"), Set.of(
                new WorkItem("work-item-3", new JobId("job-0")))
        );

        Results results = reassignmentResults(available, previous);
        assertEquals(1, results.evenlySpreadReassignmentNumber);
        assertEquals(2, results.rendezvousReassignmentNumber);
    }

    @Test
    void balanceAlreadyBalancedItems() {
        AssignmentState available = new AssignmentState();
        AssignmentState previous = new AssignmentState();

        Set<WorkItem> workItems = generateWorkItems(new JobId("job-0"), 0, 6);
        available.addWorkItems(new WorkerId("worker-0"), workItems);
        available.addWorkItems(new WorkerId("worker-1"), workItems);
        available.addWorkItems(new WorkerId("worker-2"), workItems);

        previous.addWorkItems(new WorkerId("worker-0"), Set.of(
                new WorkItem("work-item-0", new JobId("job-0")),
                new WorkItem("work-item-1", new JobId("job-0"))
        ));
        previous.addWorkItems(new WorkerId("worker-1"), Set.of(
                new WorkItem("work-item-2", new JobId("job-0")),
                new WorkItem("work-item-3", new JobId("job-0"))
        ));
        previous.addWorkItems(new WorkerId("worker-2"), Set.of(
                new WorkItem("work-item-4", new JobId("job-0")),
                new WorkItem("work-item-5", new JobId("job-0"))
        ));

        Results results = reassignmentResults(available, previous);
        assertEquals(0, results.evenlySpreadReassignmentNumber);
        assertEquals(2, results.rendezvousReassignmentNumber);
    }

    @Test
    void balanceItemsWhenWorkItemsOfJobNotBalanced() {
        AssignmentState available = new AssignmentState();
        AssignmentState previous = new AssignmentState();

        Set<WorkItem> workItems = Set.of(
                new WorkItem("work-item-0", new JobId("job-0")),
                new WorkItem("work-item-1", new JobId("job-0")),
                new WorkItem("work-item-2", new JobId("job-1")),
                new WorkItem("work-item-3", new JobId("job-1")),
                new WorkItem("work-item-4", new JobId("job-1")),
                new WorkItem("work-item-5", new JobId("job-1"))
        );
        available.addWorkItems(new WorkerId("worker-0"), workItems);
        available.addWorkItems(new WorkerId("worker-1"), workItems);
        available.addWorkItems(new WorkerId("worker-2"), workItems);

        previous.addWorkItems(new WorkerId("worker-0"), Set.of(
                new WorkItem("work-item-0", new JobId("job-0")),
                new WorkItem("work-item-1", new JobId("job-0"))
        ));
        previous.addWorkItems(new WorkerId("worker-1"), Set.of(
                new WorkItem("work-item-2", new JobId("job-1")),
                new WorkItem("work-item-3", new JobId("job-1"))
        ));
        previous.addWorkItems(new WorkerId("worker-2"), Set.of(
                new WorkItem("work-item-4", new JobId("job-1")),
                new WorkItem("work-item-5", new JobId("job-1"))
        ));

        Results results = reassignmentResults(available, previous);
        assertEquals(2, results.evenlySpreadReassignmentNumber);
        assertEquals(3, results.rendezvousReassignmentNumber);
    }

    @Test
    void balanceItemsOfThreeJobsWhenNewWorkerStarted() {
        AssignmentState available = new AssignmentState();
        AssignmentState previous = new AssignmentState();

        Set<WorkItem> workItems = Set.of(
                new WorkItem("work-item-0", new JobId("job-0")),
                new WorkItem("work-item-1", new JobId("job-1")),
                new WorkItem("work-item-2", new JobId("job-1")),
                new WorkItem("work-item-3", new JobId("job-1")),
                new WorkItem("work-item-4", new JobId("job-1")),
                new WorkItem("work-item-5", new JobId("job-1")),
                new WorkItem("work-item-6", new JobId("job-1")),
                new WorkItem("work-item-7", new JobId("job-2")),
                new WorkItem("work-item-8", new JobId("job-2"))
        );
        available.addWorkItems(new WorkerId("worker-0"), workItems);
        available.addWorkItems(new WorkerId("worker-1"), workItems);
        available.addWorkItems(new WorkerId("worker-2"), workItems);

        previous.addWorkItems(new WorkerId("worker-0"), Set.of(
                new WorkItem("work-item-1", new JobId("job-1")),
                new WorkItem("work-item-2", new JobId("job-1")),
                new WorkItem("work-item-7", new JobId("job-2")),
                new WorkItem("work-item-3", new JobId("job-1"))
        ));
        previous.addWorkItems(new WorkerId("worker-1"), Set.of(
                new WorkItem("work-item-4", new JobId("job-1")),
                new WorkItem("work-item-5", new JobId("job-1")),
                new WorkItem("work-item-6", new JobId("job-1")),
                new WorkItem("work-item-7", new JobId("job-2")),
                new WorkItem("work-item-0", new JobId("job-0"))

        ));

        Results results = reassignmentResults(available, previous);
        assertEquals(3, results.evenlySpreadReassignmentNumber);
        assertEquals(6, results.rendezvousReassignmentNumber);
    }

    @Test
    void balanceItemsOfFourJobsWhenNewWorkerStarted() {
        AssignmentState available = new AssignmentState();
        AssignmentState previous = new AssignmentState();

        Set<WorkItem> workItems = Set.of(
                new WorkItem("work-item-0", new JobId("job-0")),
                new WorkItem("work-item-1", new JobId("job-0")),
                new WorkItem("work-item-2", new JobId("job-1")),
                new WorkItem("work-item-3", new JobId("job-1")),
                new WorkItem("work-item-4", new JobId("job-2")),
                new WorkItem("work-item-5", new JobId("job-2")),
                new WorkItem("work-item-6", new JobId("job-3")),
                new WorkItem("work-item-7", new JobId("job-3"))
        );
        available.addWorkItems(new WorkerId("worker-0"), workItems);
        available.addWorkItems(new WorkerId("worker-1"), workItems);
        available.addWorkItems(new WorkerId("worker-2"), workItems);

        previous.addWorkItems(new WorkerId("worker-0"), Set.of(
                new WorkItem("work-item-0", new JobId("job-0")),
                new WorkItem("work-item-2", new JobId("job-1")),
                new WorkItem("work-item-4", new JobId("job-2")),
                new WorkItem("work-item-6", new JobId("job-3"))
        ));
        previous.addWorkItems(new WorkerId("worker-1"), Set.of(
                new WorkItem("work-item-1", new JobId("job-0")),
                new WorkItem("work-item-3", new JobId("job-1")),
                new WorkItem("work-item-5", new JobId("job-2")),
                new WorkItem("work-item-6", new JobId("job-3"))
        ));

        Results results = reassignmentResults(available, previous);
        assertEquals(2, results.evenlySpreadReassignmentNumber);
        assertEquals(7, results.rendezvousReassignmentNumber);
    }

    @Test
    void balanceItemsOfFourJobsWhenWasAliveSingleWorkerAndNewWorkerStarted() {
        AssignmentState available = new AssignmentState();
        AssignmentState previous = new AssignmentState();

        Set<WorkItem> workItems = Set.of(
                new WorkItem("work-item-0", new JobId("job-0")),
                new WorkItem("work-item-1", new JobId("job-0")),
                new WorkItem("work-item-2", new JobId("job-1")),
                new WorkItem("work-item-3", new JobId("job-1")),
                new WorkItem("work-item-4", new JobId("job-2")),
                new WorkItem("work-item-5", new JobId("job-2")),
                new WorkItem("work-item-6", new JobId("job-3")),
                new WorkItem("work-item-7", new JobId("job-3"))
        );
        available.addWorkItems(new WorkerId("worker-0"), workItems);
        available.addWorkItems(new WorkerId("worker-1"), workItems);

        previous.addWorkItems(new WorkerId("worker-0"), workItems);

        Results results = reassignmentResults(available, previous);
        assertEquals(4, results.evenlySpreadReassignmentNumber);
        assertEquals(5, results.rendezvousReassignmentNumber);
    }

    @Test
    void balanceItemsOfFourJobsWhenWasAliveSingleWorkerAndNewFiveWorkersStarted() {
        AssignmentState available = new AssignmentState();
        AssignmentState previous = new AssignmentState();

        Set<WorkItem> workItems = Set.of(
                new WorkItem("work-item-0", new JobId("job-0")),
                new WorkItem("work-item-1", new JobId("job-0")),
                new WorkItem("work-item-2", new JobId("job-0")),
                new WorkItem("work-item-3", new JobId("job-0")),
                new WorkItem("work-item-4", new JobId("job-0")),
                new WorkItem("work-item-5", new JobId("job-1")),
                new WorkItem("work-item-6", new JobId("job-1")),
                new WorkItem("work-item-7", new JobId("job-1")),
                new WorkItem("work-item-8", new JobId("job-1")),
                new WorkItem("work-item-9", new JobId("job-1")),
                new WorkItem("work-item-10", new JobId("job-2")),
                new WorkItem("work-item-11", new JobId("job-3")),
                new WorkItem("work-item-12", new JobId("job-3"))
        );
        available.addWorkItems(new WorkerId("worker-0"), workItems);
        available.addWorkItems(new WorkerId("worker-1"), workItems);
        available.addWorkItems(new WorkerId("worker-2"), workItems);
        available.addWorkItems(new WorkerId("worker-3"), workItems);
        available.addWorkItems(new WorkerId("worker-4"), workItems);
        available.addWorkItems(new WorkerId("worker-5"), workItems);

        previous.addWorkItems(new WorkerId("worker-0"), workItems);

        Results results = reassignmentResults(available, previous);
        assertEquals(10, results.evenlySpreadReassignmentNumber);
        assertEquals(10, results.rendezvousReassignmentNumber);
    }


    @Test
    void balanceItemsOfSingleJobWhenWorkerDestroyed() {
        AssignmentState available = new AssignmentState();
        AssignmentState previous = new AssignmentState();

        Set<WorkItem> workItems = Set.of(
                new WorkItem("work-item-0", new JobId("job-1")),
                new WorkItem("work-item-1", new JobId("job-1")),
                new WorkItem("work-item-2", new JobId("job-1")),
                new WorkItem("work-item-3", new JobId("job-1")),
                new WorkItem("work-item-4", new JobId("job-1")),
                new WorkItem("work-item-5", new JobId("job-1"))
        );
        available.addWorkItems(new WorkerId("worker-0"), workItems);
        available.addWorkItems(new WorkerId("worker-1"), workItems);

        previous.addWorkItems(new WorkerId("worker-0"), Set.of(
                new WorkItem("work-item-0", new JobId("job-1")),
                new WorkItem("work-item-1", new JobId("job-1"))
        ));
        previous.addWorkItems(new WorkerId("worker-1"), Set.of(
                new WorkItem("work-item-2", new JobId("job-1")),
                new WorkItem("work-item-3", new JobId("job-1"))
        ));
        previous.addWorkItems(new WorkerId("worker-2"), Set.of(
                new WorkItem("work-item-4", new JobId("job-1")),
                new WorkItem("work-item-5", new JobId("job-1"))
        ));

        Results results = reassignmentResults(available, previous);
        assertEquals(2, results.evenlySpreadReassignmentNumber);
        assertEquals(4, results.rendezvousReassignmentNumber);
    }

    @Test
    void balanceItemsOfSomeJobWhenWasAliveThreeWorkersAndOneWorkerDestroyed() {
        AssignmentState available = new AssignmentState();
        AssignmentState previous = new AssignmentState();

        Set<WorkItem> workItems = Set.of(
                new WorkItem("work-item-0", new JobId("job-0")),
                new WorkItem("work-item-1", new JobId("job-0")),
                new WorkItem("work-item-2", new JobId("job-0")),
                new WorkItem("work-item-3", new JobId("job-1")),
                new WorkItem("work-item-4", new JobId("job-1")),
                new WorkItem("work-item-5", new JobId("job-1")),
                new WorkItem("work-item-6", new JobId("job-1")),
                new WorkItem("work-item-7", new JobId("job-2")),
                new WorkItem("work-item-8", new JobId("job-3")),
                new WorkItem("work-item-9", new JobId("job-3")),
                new WorkItem("work-item-10", new JobId("job-3"))
        );
        available.addWorkItems(new WorkerId("worker-0"), workItems);
        available.addWorkItems(new WorkerId("worker-1"), workItems);

        previous.addWorkItems(new WorkerId("worker-0"), Set.of(
                new WorkItem("work-item-0", new JobId("job-0")),
                new WorkItem("work-item-3", new JobId("job-1")),
                new WorkItem("work-item-6", new JobId("job-1")),
                new WorkItem("work-item-9", new JobId("job-3"))
        ));
        previous.addWorkItems(new WorkerId("worker-1"), Set.of(
                new WorkItem("work-item-1", new JobId("job-0")),
                new WorkItem("work-item-3", new JobId("job-1")),
                new WorkItem("work-item-7", new JobId("job-2")),
                new WorkItem("work-item-10", new JobId("job-3"))
        ));
        previous.addWorkItems(new WorkerId("worker-2"), Set.of(
                new WorkItem("work-item-2", new JobId("job-0")),
                new WorkItem("work-item-5", new JobId("job-1")),
                new WorkItem("work-item-8", new JobId("job-3"))
        ));

        Results results = reassignmentResults(available, previous);
        assertEquals(4, results.evenlySpreadReassignmentNumber);
        assertEquals(9, results.rendezvousReassignmentNumber);
    }

    private Results reassignmentResults(AssignmentState available, AssignmentState previous) {
        AssignmentState newAssignmentEvenlySpread = evenlySpread.reassignAndBalance(
                generateAvailability(available),
                previous,
                new AssignmentState(),
                generateItemsToAssign(available)
        );
        AssignmentState newAssignmentRendezvous = rendezvous.reassignAndBalance(
                generateAvailability(available),
                previous,
                new AssignmentState(),
                generateItemsToAssign(available)
        );
        return new Results(
                calculateReassignments(previous, newAssignmentEvenlySpread),
                calculateReassignments(previous, newAssignmentRendezvous)
        );
    }
}
