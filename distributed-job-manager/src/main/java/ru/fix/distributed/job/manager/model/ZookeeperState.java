package ru.fix.distributed.job.manager.model;

import java.util.HashMap;
import java.util.Map;

/**
 * ZookeeperState represent Map with mapping job to workers with work items
 * and provide additional methods for easier Zookeeper state reconstruction
 */
public class ZookeeperState extends HashMap<JobId, Map<WorkerItem, WorkItem>> {

    public void addWorkerToJob(JobId job, WorkerItem worker) {

    }

    public void removeWorkerFromJob(JobId job, WorkerItem worker) {

    }

    public void moveWorkItem(JobId job, WorkItem workItem, WorkerItem from, WorkItem to) {

    }

    public HashMap<JobId, Map<WorkerItem, WorkItem>> getAsMap() {
        return this;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("Zookeeper state\n");

        for (Map.Entry<JobId, Map<WorkerItem, WorkItem>> job : entrySet()) {
            String jobId = job.getKey().getId();
            Map<WorkerItem, WorkItem> globalWorkPool = job.getValue();

            result.append("\t└ ").append(jobId).append("\n");

            for (Map.Entry<WorkerItem, WorkItem> workItemEntry : globalWorkPool.entrySet()) {
                String workerItem = workItemEntry.getKey().getId();
                String workItem = workItemEntry.getValue().getId();

                result.append("\t\t└ ").append(workerItem).append(" - ").append(workItem).append("\n");
            }

        }

        return result.toString();
    }

}
