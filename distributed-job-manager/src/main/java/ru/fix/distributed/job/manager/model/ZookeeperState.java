package ru.fix.distributed.job.manager.model;

import java.util.*;

/**
 * ZookeeperState represent Map with mapping workers to  work items
 * and provide additional methods for easier Zookeeper state reconstruction
 */
public class ZookeeperState extends HashMap<WorkerItem, List<WorkItem>> {

    public void addWorkItem(WorkerItem worker, WorkItem workItem) {
        if (this.containsKey(worker)) {
            List<WorkItem> workItems = new ArrayList<>(this.get(worker));
            workItems.add(workItem);
            this.put(worker, workItems);
        } else {
            this.put(worker, Arrays.asList(workItem));
        }
    }

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

    public HashMap<WorkerItem, List<WorkItem>> getAsMap() {
        return this;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("Zookeeper state\n");

        for (Map.Entry<WorkerItem, List<WorkItem>> worker : entrySet()) {
            String workerId = worker.getKey().getId();
            List<WorkItem> workItems = worker.getValue();

            result.append("\t└ ").append(workerId).append("\n");

            for (WorkItem workItem : workItems) {
                result.append("\t\t└ ").append(workItem.getJobId()).append(" - ").append(workItem.getId()).append("\n");
            }

        }

        return result.toString();
    }

}
