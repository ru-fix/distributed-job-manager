package ru.fix.distributed.job.manager.model;

import java.util.*;

/**
 * ZookeeperState represent Map with mapping workers to  work items
 * and provide additional methods for easier Zookeeper state reconstruction
 */
public class AssignmentState extends HashMap<WorkerItem, List<WorkItem>> {

    /**
     * If worker exists, add new work item to work item's list,
     * else create worker and add new work item
     */
    public void addWorkItem(WorkerItem worker, WorkItem workItem) {
        if (this.containsKey(worker)) {
            List<WorkItem> workItems = new ArrayList<>(this.get(worker));
            workItems.add(workItem);
            this.put(worker, workItems);
        } else {
            this.put(worker, Collections.singletonList(workItem));
        }
    }

    /**
     * If worker exists, add new workItems to existed work pool,
     * else create worker and put workItems
     */
    public void addWorkItems(WorkerItem worker, List<WorkItem> workItems) {
        if (this.containsKey(worker)) {
            List<WorkItem> newWorkItems = new ArrayList<>(this.get(worker));
            newWorkItems.addAll(workItems);
            this.put(worker, newWorkItems);
        } else {
            this.put(worker, workItems);
        }
    }

    /**
     * @return worker which has less work pool size (doesn't depends on job)
     */
    public WorkerItem getLessBusyWorker() {
        WorkerItem lessBusyWorker = null;
        int minWorkPool = Integer.MAX_VALUE;

        for (Map.Entry<WorkerItem, List<WorkItem>> worker : entrySet()) {
            List<WorkItem> workPool = worker.getValue();

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
    public WorkerItem getMostBusyWorker() {
        WorkerItem mostBusyWorker = null;
        int minWorkPool = Integer.MIN_VALUE;

        for (Map.Entry<WorkerItem, List<WorkItem>> worker : entrySet()) {
            List<WorkItem> workPool = worker.getValue();

            if (workPool.size() > minWorkPool) {
                minWorkPool = workPool.size();
                mostBusyWorker = worker.getKey();
            }
        }
        return mostBusyWorker;
    }

    public boolean containsWorkItem(WorkItem workItem) {
        for (Map.Entry<WorkerItem, List<WorkItem>> worker : entrySet()) {
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
    public WorkerItem getWorkerOfWorkItem(WorkItem workItem) {
        for (Map.Entry<WorkerItem, List<WorkItem>> worker : entrySet()) {
            for (WorkItem work : worker.getValue()) {
                if (workItem.equals(work)) {
                    return worker.getKey();
                }
            }
        }
        return null;
    }

    public HashMap<WorkerItem, List<WorkItem>> getAsMap() {
        return this;
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
        for (Map.Entry<WorkerItem, List<WorkItem>> worker : entrySet()) {
            int workPoolSize = worker.getValue().size();

            for (Map.Entry<WorkerItem, List<WorkItem>> worker1 : entrySet()) {
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
                .mapToInt(List::size)
                .sum();
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("Zookeeper state\n");

        for (Map.Entry<WorkerItem, List<WorkItem>> worker : entrySet()) {
            String workerId = worker.getKey().getId();
            List<WorkItem> workItems = worker.getValue();

            result.append("\t└ ").append(workerId).append("\n");

            for (WorkItem workItem : workItems) {
                result.append("\t\t└ ").append(workItem.getJobId()).append(" - ")
                        .append(workItem.getId()).append("\n");
            }
        }
        return result.toString();
    }

}
