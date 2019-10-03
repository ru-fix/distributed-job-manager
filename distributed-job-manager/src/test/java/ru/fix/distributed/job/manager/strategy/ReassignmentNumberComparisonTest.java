package ru.fix.distributed.job.manager.strategy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.fix.distributed.job.manager.model.AssignmentState;
import ru.fix.distributed.job.manager.model.JobId;
import ru.fix.distributed.job.manager.model.WorkItem;
import ru.fix.distributed.job.manager.model.WorkerId;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static ru.fix.distributed.job.manager.strategy.AssignmentStrategyUtils.*;

public class ReassignmentNumberComparisonTest {
    private EvenlySpreadAssignmentStrategy evenlySpread;
    private RendezvousHashAssignmentStrategy rendezvous;


    @BeforeEach
    void setUp() {
        evenlySpread = new EvenlySpreadAssignmentStrategy();
        rendezvous = new RendezvousHashAssignmentStrategy();
    }

    @Test
    void test1() {
        AssignmentState available = new AssignmentState();
        AssignmentState previous = new AssignmentState();

        available.addWorkItems(new WorkerId("worker-0"), Set.of(
                new WorkItem("work-item-0", new JobId("job-0")),
                new WorkItem("work-item-1", new JobId("job-0")),
                new WorkItem("work-item-2", new JobId("job-0")),
                new WorkItem("work-item-3", new JobId("job-0"))
        ));
        available.addWorkItems(new WorkerId("worker-1"), Set.of(
                new WorkItem("work-item-0", new JobId("job-0")),
                new WorkItem("work-item-1", new JobId("job-0")),
                new WorkItem("work-item-2", new JobId("job-0")),
                new WorkItem("work-item-3", new JobId("job-0"))
        ));

        previous.addWorkItems(new WorkerId("worker-0"), Set.of(
                new WorkItem("work-item-0", new JobId("job-0")),
                new WorkItem("work-item-1", new JobId("job-0")),
                new WorkItem("work-item-2", new JobId("job-0"))
        ));
        previous.addWorkItems(new WorkerId("worker-1"), Set.of(
                new WorkItem("work-item-3", new JobId("job-0")))
        );

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
        System.err.println(previous);
        System.err.println(newAssignmentEvenlySpread);
        System.err.println(newAssignmentRendezvous);

        int evenlySpreadReassignmentNumber = calculateReassignments(previous, newAssignmentEvenlySpread);
        int rendezvousReassignmentNumber = calculateReassignments(previous, newAssignmentRendezvous);
        assertEquals(1, evenlySpreadReassignmentNumber);

        System.err.println(evenlySpreadReassignmentNumber);
        System.err.println(rendezvousReassignmentNumber);
    }

    @Test
    void test2() {
        AssignmentState available = new AssignmentState();
        AssignmentState previous = new AssignmentState();

        Set<WorkItem> workPoolJob0 = Set.of(
                new WorkItem("work-item-0", new JobId("job-0")),
                new WorkItem("work-item-1", new JobId("job-0")),
                new WorkItem("work-item-2", new JobId("job-0")),
                new WorkItem("work-item-3", new JobId("job-0"))
        );
        available.addWorkItems(new WorkerId("worker-0"), workPoolJob0);
        available.addWorkItems(new WorkerId("worker-1"), workPoolJob0);
        available.addWorkItems(new WorkerId("worker-2"), workPoolJob0);

        previous.addWorkItems(new WorkerId("worker-0"), Set.of(
                new WorkItem("work-item-0", new JobId("job-0")),
                new WorkItem("work-item-1", new JobId("job-0")),
                new WorkItem("work-item-2", new JobId("job-0"))
        ));
        previous.addWorkItems(new WorkerId("worker-1"), Set.of(
                new WorkItem("work-item-3", new JobId("job-0")))
        );

        System.err.println(generateAvailability(available));
        System.err.println(generateItemsToAssign(available));

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
        int evenlySpreadReassignmentNumber = calculateReassignments(previous, newAssignmentEvenlySpread);
        int rendezvousReassignmentNumber = calculateReassignments(previous, newAssignmentRendezvous);
        assertEquals(1, evenlySpreadReassignmentNumber);

        System.err.println(previous);
        System.err.println(newAssignmentEvenlySpread + "" + evenlySpreadReassignmentNumber + "\n");
        System.err.println(newAssignmentRendezvous + "" + rendezvousReassignmentNumber + "\n");
    }

    @Test
    void test3() {
        AssignmentState available = new AssignmentState();
        AssignmentState previous = new AssignmentState();

        Set<WorkItem> workPoolJob0 = Set.of(
                new WorkItem("work-item-0", new JobId("job-0")),
                new WorkItem("work-item-1", new JobId("job-0")),
                new WorkItem("work-item-2", new JobId("job-0")),
                new WorkItem("work-item-3", new JobId("job-0")),
                new WorkItem("work-item-4", new JobId("job-0")),
                new WorkItem("work-item-5", new JobId("job-0"))
        );
        available.addWorkItems(new WorkerId("worker-0"), workPoolJob0);
        available.addWorkItems(new WorkerId("worker-1"), workPoolJob0);
        available.addWorkItems(new WorkerId("worker-2"), workPoolJob0);

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

        System.err.println(generateAvailability(available));
        System.err.println(generateItemsToAssign(available));

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
        int evenlySpreadReassignmentNumber = calculateReassignments(previous, newAssignmentEvenlySpread);
        int rendezvousReassignmentNumber = calculateReassignments(previous, newAssignmentRendezvous);
        assertEquals(0, evenlySpreadReassignmentNumber);

        System.err.println(previous);
        System.err.println(newAssignmentEvenlySpread + "" + evenlySpreadReassignmentNumber + "\n");
        System.err.println(newAssignmentRendezvous + "" + rendezvousReassignmentNumber + "\n");
    }

    @Test
    void test4() {
        AssignmentState available = new AssignmentState();
        AssignmentState previous = new AssignmentState();

        Set<WorkItem> workPoolJob0 = Set.of(
                new WorkItem("work-item-0", new JobId("job-0")),
                new WorkItem("work-item-1", new JobId("job-0")),
                new WorkItem("work-item-2", new JobId("job-1")),
                new WorkItem("work-item-3", new JobId("job-1")),
                new WorkItem("work-item-4", new JobId("job-1")),
                new WorkItem("work-item-5", new JobId("job-1"))
        );
        available.addWorkItems(new WorkerId("worker-0"), workPoolJob0);
        available.addWorkItems(new WorkerId("worker-1"), workPoolJob0);
        available.addWorkItems(new WorkerId("worker-2"), workPoolJob0);

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

        System.err.println(generateAvailability(available));
        System.err.println(generateItemsToAssign(available));

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
        int evenlySpreadReassignmentNumber = calculateReassignments(previous, newAssignmentEvenlySpread);
        int rendezvousReassignmentNumber = calculateReassignments(previous, newAssignmentRendezvous);
        assertEquals(2, evenlySpreadReassignmentNumber);

        System.err.println(previous);
        System.err.println(newAssignmentEvenlySpread + "" + evenlySpreadReassignmentNumber + "\n");
        System.err.println(newAssignmentRendezvous + "" + rendezvousReassignmentNumber + "\n");
    }

    @Test
    void test5() {
        AssignmentState available = new AssignmentState();
        AssignmentState previous = new AssignmentState();

        Set<WorkItem> workPoolJob0 = Set.of(
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
        available.addWorkItems(new WorkerId("worker-0"), workPoolJob0);
        available.addWorkItems(new WorkerId("worker-1"), workPoolJob0);
        available.addWorkItems(new WorkerId("worker-2"), workPoolJob0);

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
                new WorkItem("work-item-0", new JobId("job-0")),
                new WorkItem("work-item-8", new JobId("job-2"))

        ));

        System.err.println(generateAvailability(available));
        System.err.println(generateItemsToAssign(available));

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
        int evenlySpreadReassignmentNumber = calculateReassignments(previous, newAssignmentEvenlySpread);
        int rendezvousReassignmentNumber = calculateReassignments(previous, newAssignmentRendezvous);
        assertEquals(3, evenlySpreadReassignmentNumber);

        System.err.println(previous);
        System.err.println(newAssignmentEvenlySpread + "" + evenlySpreadReassignmentNumber + "\n");
        System.err.println(newAssignmentRendezvous + "" + rendezvousReassignmentNumber + "\n");
    }
}
