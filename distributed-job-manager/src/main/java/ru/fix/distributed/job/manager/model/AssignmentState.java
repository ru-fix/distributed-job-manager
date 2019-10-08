package ru.fix.distributed.job.manager.model;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * AssignmentState represent Map with mapping workers to  work items
 * and provide additional methods for easier manipulations with AssignmentState
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
     * @param workerId worker name, that contains work items
     * @param jobId    job name, the work item that you want to get
     * @return pool of work items of jobId, placed on workerId
     */
    public Set<WorkItem> get(WorkerId workerId, JobId jobId) {
        return this.get(workerId).stream()
                .filter(item -> item.getJobId().equals(jobId))
                .collect(Collectors.toSet());
    }

    /**
     * @return worker which has less work pool size (doesn't depends on job)
     */
    @SuppressWarnings("unused")
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
     * @param jobId            job name for filtering work items on worker
     * @param availableWorkers set of workers, that should be considered
     * @return worker from availableWorkers, that have minimal work pool size of jobId
     */
    @SuppressWarnings("unused")
    public WorkerId getLessBusyWorkerWithJobIdFromAvailableWorkers(JobId jobId, Set<WorkerId> availableWorkers) {
        WorkerId globalLessBusyWorker = getLessBusyWorkerFromAvailableWorkers(availableWorkers);
        WorkerId localLessBusyWorker = null;
        int minWorkPoolSize = Integer.MAX_VALUE;

        for (Map.Entry<WorkerId, HashSet<WorkItem>> worker : entrySet()) {
            if (!availableWorkers.contains(worker.getKey())) {
                continue;
            }
            int workPoolSize = (int) worker.getValue().stream()
                    .filter(item -> jobId.equals(item.getJobId()))
                    .count();

            if (workPoolSize <= minWorkPoolSize) {
                minWorkPoolSize = workPoolSize;
                localLessBusyWorker = worker.getKey();
            }
            if (globalLessBusyWorker.equals(localLessBusyWorker)) {
                return localLessBusyWorker;
            }
        }
        return localLessBusyWorker;
    }

    /**
     * @param availableWorkers set of workers, that should be considered
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

    /**
     * @param workItem item, for which you need to find a worker
     * @return worker on which work item placed
     */
    public WorkerId getWorkerOfWorkItem(WorkItem workItem) {
        for (Map.Entry<WorkerId, HashSet<WorkItem>> worker : entrySet()) {
            for (WorkItem item : worker.getValue()) {
                if (workItem.equals(item)) {
                    return worker.getKey();
                }
            }
        }
        return null;
    }

    /**
     * @param workItem item, which should be checked for content
     * @return true, if contains workItem
     */
    public boolean containsWorkItem(WorkItem workItem) {
        for (Map.Entry<WorkerId, HashSet<WorkItem>> worker : entrySet()) {
            if (worker.getValue().contains(workItem)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param workerId worker name, on which should be checked content
     * @param workItem item, which should be checked for content
     * @return true, if contains workItem on workerId
     */
    public boolean containsWorkItemOnWorker(WorkerId workerId, WorkItem workItem) {
        Set<WorkItem> items = get(workerId);
        if (items == null) {
            return false;
        }
        return items.contains(workItem);
    }

    /**
     * @param workerId worker name, on which should be checked content
     * @param jobId    job name for filtering work items on worker
     * @return true, if contains any work item of jobId on workerId
     */
    public boolean containsAnyWorkItemOfJob(WorkerId workerId, JobId jobId) {
        for (WorkItem item : get(workerId)) {
            if (jobId.equals(item.getJobId())) {
                return true;
            }
        }
        return false;
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

    /**
     * @param jobId job name for filtering work items on worker
     * @return true, if work pool sizes of jobId on various workers differ more than 1
     */
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

    /**
     * @return number of all work items in AssignmentState
     */
    public int globalPoolSize() {
        return this.values().stream()
                .mapToInt(HashSet::size)
                .sum();
    }

    /**
     * @param jobId job name for filtering work items on worker
     * @return number of work items of jobId in AssignmentState
     */
    @SuppressWarnings("unused")
    public int localPoolSize(JobId jobId) {
        return (int) this.values().stream()
                .flatMap(Collection::stream)
                .filter(item -> jobId.equals(item.getJobId()))
                .count();
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("Assignment state\n");

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
