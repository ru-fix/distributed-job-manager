package ru.fix.distributed.job.manager.strategy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.fix.distributed.job.manager.model.JobId;
import ru.fix.distributed.job.manager.model.WorkItem;
import ru.fix.distributed.job.manager.model.WorkerItem;
import ru.fix.distributed.job.manager.model.ZookeeperState;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EvenlySpreadAssignmentStrategyTest {
    private EvenlySpreadAssignmentStrategy evenlySpread;

    @BeforeEach
    public void setUp() {
        evenlySpread = new EvenlySpreadAssignmentStrategy();
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

        ZookeeperState newAssignment = evenlySpread.reassignAndBalance(
                available,
                previous,
                currentState,
                generateItemsToAssign(available, currentState)
        );

        assertTrue(newAssignment.isBalanced());
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

        ZookeeperState newAssignment = evenlySpread.reassignAndBalance(
                available,
                previous,
                currentState,
                generateItemsToAssign(available, currentState)
        );

        assertTrue(newAssignment.isBalanced());
    }

    @Test
    public void reassignAndBalanceIfWorkerNotAvailable() {
        ZookeeperState available = new ZookeeperState();
        ZookeeperState previous = new ZookeeperState();

        addWorkerWithItems(available, "worker-0", 3, 1);
        available.addWorkItem(new WorkerItem("worker-1"), new WorkItem("work-item-1", "job-3"));
        available.addWorkItem(new WorkerItem("worker-1"), new WorkItem("work-item-2", "job-3"));
        available.addWorkItem(new WorkerItem("worker-1"), new WorkItem("work-item-0", "job-3"));

        previous.addWorkItems(new WorkerItem("worker-0"), Arrays.asList(
                new WorkItem("work-item-1", "job-3"),
                new WorkItem("work-item-0", "job-0")
        ));


        ZookeeperState currentState = generateCurrentState(available, previous);

        assertFalse(currentState.isBalanced());

        ZookeeperState newAssignment = evenlySpread.reassignAndBalance(
                available,
                previous,
                currentState,
                generateItemsToAssign(available, currentState)
        );

        assertTrue(newAssignment.isBalanced());
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
                new WorkItem("work-item-0", "job-3"),
                new WorkItem("work-item-3", "job-0")
        ));

        ZookeeperState currentState = generateCurrentState(available, previous);

        assertFalse(currentState.isBalanced());
        System.err.println(available);
        System.err.println(previous);
        System.err.println(currentState);
        ZookeeperState newAssignment = evenlySpread.reassignAndBalance(
                available,
                previous,
                currentState,
                generateItemsToAssign(available, currentState)
        );
        System.err.println(newAssignment);
        assertTrue(newAssignment.isBalanced());
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

        ZookeeperState newAssignment = evenlySpread.reassignAndBalance(
                available,
                previous,
                currentState,
                generateItemsToAssign(available, currentState)
        );

        assertTrue(newAssignment.isBalanced());
    }

    private void addWorkerWithItems(ZookeeperState state, String worker, int workItemsCount, int jobsCount) {
        List<WorkItem> workItems = new ArrayList<>();

        for (int i = 0; i < workItemsCount; i++) {
            for (int j = 0; j < jobsCount; j++) {
                workItems.add(new WorkItem("work-item-" + i, "job-" + j));
            }
        }
        state.addWorkItems(new WorkerItem(worker), workItems);
    }

    private ZookeeperState generateCurrentState(ZookeeperState available, ZookeeperState current) {
        ZookeeperState newAssignment = new ZookeeperState();

        for (Map.Entry<WorkerItem, List<WorkItem>> worker : current.entrySet()) {
            if (available.containsKey(worker.getKey())) {
                newAssignment.addWorkItems(worker.getKey(), worker.getValue());
            }
        }
        for (Map.Entry<WorkerItem, List<WorkItem>> worker : available.entrySet()) {
            if (!current.containsKey(worker.getKey())) {
                newAssignment.addWorkItems(worker.getKey(), Collections.emptyList());
            }
        }
        return newAssignment;
    }

    private Map<JobId, List<WorkItem>> generateItemsToAssign(
            ZookeeperState availableState,
            ZookeeperState currentState
    ) {
        Map<JobId, List<WorkItem>> workItemsToAssign = new HashMap<>();

        for (Map.Entry<WorkerItem, List<WorkItem>> worker : availableState.entrySet()) {
            for (WorkItem workItem : worker.getValue()) {
                String jobId = workItem.getJobId();

                if (currentState.containsWorkItem(workItem)) {
                    continue;
                }

                if (workItemsToAssign.containsKey(new JobId(jobId))) {
                    List<WorkItem> workItemsOld = new ArrayList<>(workItemsToAssign.get(new JobId(jobId)));
                    workItemsOld.add(workItem);
                    workItemsToAssign.put(new JobId(jobId), workItemsOld);
                } else {
                    workItemsToAssign.put(new JobId(jobId), Collections.singletonList(workItem));
                }
            }
        }
        return workItemsToAssign;
    }
}