package ru.fix.distributed.job.manager.model;

import java.util.*;
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
    @SuppressWarnings("unused")
    public void addWorkItems(WorkerId worker, Set<WorkItem> workItems) {
        this.computeIfAbsent(worker, key -> new HashSet<>()).addAll(workItems);
    }

    /**
     * Remove workItem from worker
     */
    public void removeWorkItem(WorkerId worker, WorkItem workItem) {
        this.computeIfAbsent(worker, key -> new HashSet<>()).remove(workItem);
    }


    /**
     * @param workerId worker name, that contains work items
     * @param jobId    job name, the work item that you want to get
     * @return pool of work items of jobId, placed on workerId
     */
    public Set<WorkItem> getWorkItems(WorkerId workerId, JobId jobId) {
        return this.get(workerId).stream()
                .filter(item -> item.getJobId().equals(jobId))
                .collect(Collectors.toSet());
    }

    /**
     * Move item from workerFrom to workerTo
     */
    public void moveWorkItem(WorkItem item, WorkerId workerFrom, WorkerId workerTo) {
        this.removeWorkItem(workerFrom, item);
        this.addWorkItem(workerTo, item);
    }

    /**
     * @return worker which has less work pool size (doesn't depends on job)
     * or return null, if assignment state doesn't contain any worker
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
     * or null, if assignment state or availableWorkers are empty
     */
    @SuppressWarnings("unused")
    public WorkerId getLessBusyWorkerWithJobId(JobId jobId, Set<WorkerId> availableWorkers) {
        WorkerId localLessBusyWorker = null;
        int minWorkPoolSize = Integer.MAX_VALUE;

        for (Map.Entry<WorkerId, HashSet<WorkItem>> worker : entrySet()) {
            if (!availableWorkers.contains(worker.getKey())) {
                continue;
            }
            int workPoolSize = localPoolSize(jobId, worker.getKey());
            if (workPoolSize <= minWorkPoolSize) {
                minWorkPoolSize = workPoolSize;
                localLessBusyWorker = worker.getKey();
            }
        }
        return localLessBusyWorker;
    }

    /**
     * @param availableWorkers set of workers, that should be considered
     * @return worker from availableWorkers which has less work pool size (doesn't depends on job)
     * or null, if assignment state or availableWorkers are empty
     */
    public WorkerId getLessBusyWorker(Set<WorkerId> availableWorkers) {
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
     * @param jobId            job name for filtering work items on worker
     * @param availableWorkers set of workers, that should be considered
     * @return worker from availableWorkers, that have maximal work pool size of jobId
     * *  or null, if assignment state or availableWorkers are empty
     */
    public WorkerId getMostBusyWorkerWithJobId(JobId jobId, Set<WorkerId> availableWorkers) {
        WorkerId mostBusyWorker = null;
        int maxWorkPoolSize = Integer.MIN_VALUE;

        for (Map.Entry<WorkerId, HashSet<WorkItem>> worker : entrySet()) {
            if (!availableWorkers.contains(worker.getKey())) {
                continue;
            }
            int workPoolSize = localPoolSize(jobId, worker.getKey());
            if (workPoolSize >= maxWorkPoolSize) {
                maxWorkPoolSize = workPoolSize;
                mostBusyWorker = worker.getKey();
            }
        }
        return mostBusyWorker;
    }

    /**
     * @param availableWorkers set of workers, that should be considered
     * @return worker from availableWorkers, that have maximal work pool size of jobId
     * *  or null, if assignment state or availableWorkers are empty
     */
    public WorkerId getMostBusyWorker(Set<WorkerId> availableWorkers) {
        WorkerId mostBusyWorker = null;
        int maxWorkPoolSize = Integer.MIN_VALUE;

        for (Map.Entry<WorkerId, HashSet<WorkItem>> worker : entrySet()) {
            if (!availableWorkers.contains(worker.getKey())) {
                continue;
            }
            int workPoolSize = worker.getValue().size();

            if (workPoolSize >= maxWorkPoolSize) {
                maxWorkPoolSize = workPoolSize;
                mostBusyWorker = worker.getKey();
            }
        }
        return mostBusyWorker;
    }

    /**
     * @param workItem item, for which you need to find a worker
     * @return worker on which work item placed
     * or null, if work item not found
     */
    public WorkerId getWorkerOfWorkItem(WorkItem workItem) {
        for (Map.Entry<WorkerId, HashSet<WorkItem>> worker : entrySet()) {
            if (worker.getValue().contains(workItem)) {
                return worker.getKey();
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

    public boolean isBalanced() {
        return isBalanced(this.keySet());
    }

    /**
     * @return true, if work pool sizes of various workers differ more than gap size
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
    public boolean isBalanced(Set<WorkerId> availableWorkers) {
        int minPoolSize = Integer.MAX_VALUE;
        int maxPoolSize = Integer.MIN_VALUE;

        for (Map.Entry<WorkerId, HashSet<WorkItem>> worker : entrySet()) {
            if (!availableWorkers.contains(worker.getKey())) {
                continue;
            }
            int workPoolSize = worker.getValue().size();
            if (workPoolSize > maxPoolSize) {
                maxPoolSize = workPoolSize;
            }
            if (workPoolSize < minPoolSize) {
                minPoolSize = workPoolSize;
            }
        }
        return maxPoolSize - minPoolSize <= 1;
    }

    /**
     * @param jobId job name for filtering work items on worker
     * @return true, if work pool sizes of jobId on various workers differ more than 1
     */
    public boolean isBalancedByJobId(JobId jobId, Set<WorkerId> availableWorkers) {
        int minPoolSize = Integer.MAX_VALUE;
        int maxPoolSize = Integer.MIN_VALUE;

        for (Map.Entry<WorkerId, HashSet<WorkItem>> worker : entrySet()) {
            if (!availableWorkers.contains(worker.getKey())) {
                continue;
            }
            int workPoolSize = (int) worker.getValue().stream()
                    .filter(item -> jobId.equals(item.getJobId())).count();

            if (workPoolSize > maxPoolSize) {
                maxPoolSize = workPoolSize;
            }
            if (workPoolSize < minPoolSize) {
                minPoolSize = workPoolSize;
            }
        }
        return maxPoolSize - minPoolSize <= 1;
    }

    /**
     * @param availability shows on which workers job can be launched
     * @return true, if each job of assignment state is balanced
     */
    public boolean isBalancedForEachJob(Map<JobId, Set<WorkerId>> availability) {
        for (Map.Entry<JobId, Set<WorkerId>> availabilityEntry : availability.entrySet()) {
            JobId jobId = availabilityEntry.getKey();
            Set<WorkerId> availableWorkers = availabilityEntry.getValue();

            if (!this.isBalancedByJobId(jobId, availableWorkers)) {
                return false;
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

    private int localPoolSize(JobId jobId, WorkerId workerId) {
        return (int) this.get(workerId).stream()
                .filter(item -> item.getJobId().equals(jobId))
                .count();
    }

    public String getWorkPoolSizeInfo(Map<JobId, Set<WorkerId>> availability) {
        StringBuilder info = new StringBuilder("Load per job:\n");
        availability.forEach((jobId, availableWorkers) -> {
            info.append(jobId)
                    .append(" - work pool size: ")
                    .append(localPoolSize(jobId))
                    .append("\n");
            availableWorkers.forEach(workerId ->
                    info.append("\t")
                            .append(workerId)
                            .append(": ")
                            .append(this.getWorkItems(workerId, jobId).size())
                            .append("\n")
            );
        });
        info.append("Global load:\n");
        this.forEach((workerId, items) ->
                info.append("\t")
                        .append(workerId)
                        .append(": ")
                        .append(items.size())
                        .append("\n")
        );
        return info.toString();
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("assignment state:\n");
        List<Entry<WorkerId, HashSet<WorkItem>>> sortedAssignment = new ArrayList<>(entrySet());
        sortedAssignment.sort(Comparator.comparing(Entry::getKey));

        for (Map.Entry<WorkerId, HashSet<WorkItem>> worker : sortedAssignment) {
            String workerId = worker.getKey().getId();
            List<WorkItem> sortedWorkItems = new ArrayList<>(worker.getValue());
            sortedWorkItems.sort(Comparator.comparing(o -> (o.getJobId().getId() + "" + o.getId())));

            result.append("\t└ ").append(workerId).append("\n");

            for (WorkItem workItem : sortedWorkItems) {
                result.append("\t\t└ ").append(workItem.getJobId()).append(" - ")
                        .append(workItem.getId()).append("\n");
            }
        }
        return result.toString();
    }

}
