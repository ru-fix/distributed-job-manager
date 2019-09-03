package ru.fix.distributed.job.manager.model.distribution;

import java.util.HashMap;
import java.util.Map;

/**
 * ZookeeperState represent Map with mapping job to workers with work items
 *  and provide additional methods for easier Zookeeper state reconstruction
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
}
