package ru.fix.distributed.job.manager.strategy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.fix.distributed.job.manager.model.JobId;
import ru.fix.distributed.job.manager.model.WorkItem;
import ru.fix.distributed.job.manager.model.WorkerItem;
import ru.fix.distributed.job.manager.model.ZookeeperState;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class RendezvousHashAssignmentStrategyTest {
    private RendezvousHashAssignmentStrategy rendezvous;

    @BeforeEach
    void setUp() {
        rendezvous = new RendezvousHashAssignmentStrategy();
    }

    @Test
    void reassignAndBalanceWhenOnlyOneWorkerHasJobs() {
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

        Map<JobId, List<WorkItem>> itemsToAssign = generateItemsToAssign(available, previous);

        assertFalse(available.isBalanced());

        ZookeeperState newAssignment = rendezvous.reassignAndBalance(
                available,
                previous,
                previous,
                itemsToAssign
        );

        System.err.println(newAssignment);

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

        Map<JobId, List<WorkItem>> itemsToAssign = generateItemsToAssign(available, previous);

        assertFalse(available.isBalanced());

        ZookeeperState newAssignment = rendezvous.reassignAndBalance(
                available,
                previous,
                previous,
                itemsToAssign
        );
        System.err.println(newAssignment);
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

    private Map<JobId, List<WorkItem>> generateItemsToAssign(
            ZookeeperState availableState,
            ZookeeperState currentState
    ) {
        Map<JobId, List<WorkItem>> workItemsToAssign = new HashMap<>();

        for (Map.Entry<WorkerItem, List<WorkItem>> worker : availableState.entrySet()) {
            WorkerItem workerItem = worker.getKey();

            for (WorkItem workItem : worker.getValue()) {
                String jobId = workItem.getJobId();

                if (currentState.containsWorkItem(workItem) &&
                        workerItem.equals(currentState.getWorkerOfWorkItem(workItem))) {
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