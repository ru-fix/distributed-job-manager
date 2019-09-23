package ru.fix.distributed.job.manager.model;

import java.util.*;

/**
 * ZookeeperState represent Map with mapping workers to  work items
 * and provide additional methods for easier Zookeeper state reconstruction
 */
public class AssignmentState extends HashMap<WorkerId, HashSet<WorkItem>> {

    /**
     * If worker exists, add new work item to work item's list,
     * else create worker and add new work item
     */
    public void addWorkItem(WorkerId worker, WorkItem workItem) {
        this.computeIfAbsent(worker, key -> new HashSet<>()).add(workItem);
    }

    /**
     * If worker exists, add new workItems to existed work pool,
     * else create worker and put workItems
     */
    public void addWorkItems(WorkerId worker, Set<WorkItem> workItems) {
        this.computeIfAbsent(worker, key -> new HashSet<>()).addAll(workItems);
    }

    /**
     * @return worker which has less work pool size (doesn't depends on job)
     */
    public WorkerId getLessBusyWorker() {
        WorkerId lessBusyWorker = null;
        int minWorkPool = Integer.MAX_VALUE;

        for (Map.Entry<WorkerId, HashSet<WorkItem>> worker : entrySet()) {
            HashSet<WorkItem> workPool = worker.getValue();

            if (workPool.size() < minWorkPool) {
                minWorkPool = workPool.size();
                lessBusyWorker = worker.getKey();
            }
        }
        return lessBusyWorker;
    }

    /**
     * @return worker which has most work pool size (doesn't depends on job)
     */
    public WorkerId getMostBusyWorker() {
        WorkerId mostBusyWorker = null;
        int minWorkPool = Integer.MIN_VALUE;

        for (Map.Entry<WorkerId, HashSet<WorkItem>> worker : entrySet()) {
            HashSet<WorkItem> workPool = worker.getValue();

            if (workPool.size() > minWorkPool) {
                minWorkPool = workPool.size();
                mostBusyWorker = worker.getKey();
            }
        }
        return mostBusyWorker;
    }

    public boolean containsWorkItem(WorkItem workItem) {
        for (Map.Entry<WorkerId, HashSet<WorkItem>> worker : entrySet()) {
            for (WorkItem work : worker.getValue()) {
                if (workItem.equals(work)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * @return worker on which work item placed
     */
    public WorkerId getWorkerOfWorkItem(WorkItem workItem) {
        for (Map.Entry<WorkerId, HashSet<WorkItem>> worker : entrySet()) {
            for (WorkItem work : worker.getValue()) {
                if (workItem.equals(work)) {
                    return worker.getKey();
                }
            }
        }
        return null;
    }

    /**
     * @return true, if work pool sizes of various workers differ more than 1
     * For example:
     * worker-1: 4
     * worker-2: 5
     * worker-3: 5
     * is balanced, returns true
     * worker-1: 0
     * worker-2: 0
     * returns also true
     * worker-1: 4
     * worker-2: 5
     * worker-3: 3
     * returns false
     */
    public boolean isBalanced() {
        for (Map.Entry<WorkerId, HashSet<WorkItem>> worker : entrySet()) {
            int workPoolSize = worker.getValue().size();

            for (Map.Entry<WorkerId, HashSet<WorkItem>> worker1 : entrySet()) {
                int workPoolSize1 = worker1.getValue().size();

                if (workPoolSize - workPoolSize1 > 1) {
                    return false;
                }
            }
        }
        return true;
    }

    public int globalPoolSize() {
        return this.values().stream()
                .mapToInt(HashSet::size)
                .sum();
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("Zookeeper state\n");

        for (Map.Entry<WorkerId, HashSet<WorkItem>> worker : entrySet()) {
            String workerId = worker.getKey().getId();
            HashSet<WorkItem> workItems = worker.getValue();

            result.append("\t└ ").append(workerId).append("\n");

            for (WorkItem workItem : workItems) {
                result.append("\t\t└ ").append(workItem.getJobId()).append(" - ")
                        .append(workItem.getId()).append("\n");
            }
        }
        return result.toString();
    }

}
