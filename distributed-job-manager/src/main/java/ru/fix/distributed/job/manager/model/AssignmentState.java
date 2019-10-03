package ru.fix.distributed.job.manager.model;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

    public Set<WorkItem> get(WorkerId workerId, JobId jobId) {
        return this.get(workerId).stream()
                .filter(item -> item.getJobId().equals(jobId))
                .collect(Collectors.toSet());
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

    public WorkerId getLessBusyWorkerWithJobIdFromAvailableWorkers(JobId jobId, Set<WorkerId> availableWorkers) {
        WorkerId lessBusyWorker = null;
        int minWorkPoolSize = Integer.MAX_VALUE;

        WorkerId globalLessBusyWorker = getLessBusyWorkerFromAvailableWorkers(availableWorkers);

        for (Map.Entry<WorkerId, HashSet<WorkItem>> worker : entrySet()) {
            if (!availableWorkers.contains(worker.getKey())) {
                continue;
            }
            int workPoolSize = (int) worker.getValue().stream()
                    .filter(item -> jobId.equals(item.getJobId()))
                    .count();

            if (workPoolSize <= minWorkPoolSize) {
                minWorkPoolSize = workPoolSize;
                lessBusyWorker = worker.getKey();
            }
            if (globalLessBusyWorker.equals(lessBusyWorker)) {
                return lessBusyWorker;
            }
        }
        return lessBusyWorker;
    }

    /**
     * @return worker from availableWorkers which has less work pool size (doesn't depends on job)
     */
    public WorkerId getLessBusyWorkerFromAvailableWorkers(Set<WorkerId> availableWorkers) {
        WorkerId lessBusyWorker = null;
        int minWorkPool = Integer.MAX_VALUE;

        for (Map.Entry<WorkerId, HashSet<WorkItem>> worker : entrySet()) {
            if (!availableWorkers.contains(worker.getKey())) {
                continue;
            }
            HashSet<WorkItem> workPool = worker.getValue();

            if (workPool.size() < minWorkPool) {
                minWorkPool = workPool.size();
                lessBusyWorker = worker.getKey();
            }
        }
        return lessBusyWorker;
    }

    public Set<WorkerId> getLocalMinimums(JobId jobId, Set<WorkerId> availableWorkers) {
        WorkerId workerId = this.getLessBusyWorkerWithJobIdFromAvailableWorkers(jobId, availableWorkers);
        int minWorkPoolSize = this.get(workerId).size();
        Set<WorkerId> localMinimums = new HashSet<>();

        for (Map.Entry<WorkerId, HashSet<WorkItem>> worker : entrySet()) {
            if (!availableWorkers.contains(worker.getKey())) {
                continue;
            }
            int workPoolSize = (int) worker.getValue().stream()
                    .filter(item -> jobId.equals(item.getJobId()))
                    .count();

            if (workPoolSize < minWorkPoolSize) {
                localMinimums.add(worker.getKey());
            }
        }
        return localMinimums;
    }

    public Set<WorkerId> getLGlobalMinimums(Set<WorkerId> availableWorkers) {
        WorkerId workerId = this.getLessBusyWorkerFromAvailableWorkers(availableWorkers);
        int minWorkPoolSize = this.get(workerId).size();
        Set<WorkerId> globalMinimums = new HashSet<>();

        for (Map.Entry<WorkerId, HashSet<WorkItem>> worker : entrySet()) {
            if (!availableWorkers.contains(worker.getKey())) {
                continue;
            }
            HashSet<WorkItem> workPool = worker.getValue();

            if (workPool.size() < minWorkPoolSize) {
                globalMinimums.add(worker.getKey());
            }
        }
        return globalMinimums;
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
            if (worker.getValue().contains(workItem)) {
                return true;
            }
        }
        return false;
    }

    public boolean containsAnyWorkItemOfJob(WorkerId workerId, JobId jobId) {
        for (WorkItem item : get(workerId)) {
            if (jobId.equals(item.getJobId())) {
                return true;
            }
        }

        return false;
    }

    public boolean containsWorkItemOnWorker(WorkerId workerId, WorkItem workItem) {
        for (WorkItem item : get(workerId)) {
            if (workItem.equals(item)) {
                return true;
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

                if (Math.abs(workPoolSize - workPoolSize1) > 1) {
                    return false;
                }
            }
        }
        return true;
    }

    public boolean isBalancedByJobId(JobId jobId) {
        for (Map.Entry<WorkerId, HashSet<WorkItem>> worker : entrySet()) {
            long workPoolSize = worker.getValue().stream()
                    .filter(item -> jobId.equals(item.getJobId())).count();

            for (Map.Entry<WorkerId, HashSet<WorkItem>> worker1 : entrySet()) {
                long workPoolSize1 = worker1.getValue().stream()
                        .filter(item -> jobId.equals(item.getJobId())).count();

                if (Math.abs(workPoolSize - workPoolSize1) > 1) {
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
