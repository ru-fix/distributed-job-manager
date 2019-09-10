package ru.fix.distributed.job.manager.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.distributed.job.manager.model.*;

import java.util.*;
import java.util.stream.Collectors;

public class DefaultAssignmentStrategy implements AssignmentStrategy {

    private static final Logger logger = LoggerFactory.getLogger(DefaultAssignmentStrategy.class);

    public JobState reassignAndBalance(JobState jobAvailability, JobState currentAssignment) {
        // create work pool to workers map
        Map<WorkItem, Set<WorkerItem>> workPoolAvailableWorkers = new HashMap<>();
        jobAvailability.getWorkers()
                .forEach(workerItem -> workerItem.getWorkPools()
                        .forEach(workPoolItem -> workPoolAvailableWorkers
                                .computeIfAbsent(workPoolItem, v -> new HashSet<>()).add(workerItem)));

        // sort work pool to workers map
        NavigableMap<WorkItem, Set<WorkerItem>> sortedWorkPoolAvailableWorkers = new TreeMap<>((j1, j2) -> {
            int freedomComparison = Integer.compare(workPoolAvailableWorkers.get(j1).size(),
                    workPoolAvailableWorkers.get(j2).size());
            if (freedomComparison != 0) {
                return freedomComparison;
            } else {
                return j1.getId().compareTo(j2.getId());
            }
        });
        sortedWorkPoolAvailableWorkers.putAll(workPoolAvailableWorkers);

        // assign started from less free work pools
        Map<WorkerItem, Set<WorkItem>> currentAssignmentMap = currentAssignment.toMap();
        Map<WorkerItem, Set<WorkItem>> newAssignmentMap = jobAvailability.getWorkers().stream()
                .collect(Collectors.toMap(k -> k, v -> new HashSet<>()));
        sortedWorkPoolAvailableWorkers.forEach((workPoolItem, workersItem) -> {
            Optional<WorkerItem> worker = workersItem.stream().min((w1, w2) -> {
                int workerWorkLoadComparison = Integer.compare(
                        newAssignmentMap.get(w1).size(),
                        newAssignmentMap.get(w2).size());
                if (workerWorkLoadComparison != 0) {
                    return workerWorkLoadComparison;
                } else {
                    if (currentAssignmentMap.getOrDefault(w1, Collections.emptySet()).contains(workPoolItem)) {
                        return -1;
                    } else if (currentAssignmentMap.getOrDefault(w2, Collections.emptySet()).contains(workPoolItem)) {
                        return 1;
                    } else {
                        return w1.getId().compareTo(w2.getId());
                    }
                }
            });

            if (!worker.isPresent()) {
                throw new IllegalStateException("Current workers does not allow to assign work pool "
                        + workPoolItem + "; jobAvailability: " + jobAvailability + "; currentAssignment: "
                        + currentAssignment);
            }
            newAssignmentMap.get(worker.get()).add(workPoolItem);
        });

        // copy original items into new assignment state
        JobState newAssignmentState = new JobState();
        newAssignmentMap.forEach((origWorkerItem, origWorkPoolItems) -> {
            WorkerItem workerItem = new WorkerItem(origWorkerItem.getId());
            newAssignmentState.getWorkers().add(workerItem);

            origWorkPoolItems.forEach(origWorkPoolItem -> workerItem.getWorkPools()
                    .add(new WorkItem(origWorkPoolItem.getId())));
        });

        logger.trace("Distributed {} \n to {} ", jobAvailability, newAssignmentState);

        return newAssignmentState;
    }

    @Override
    public ZookeeperState reassignAndBalance(ZookeeperState availability, ZookeeperState prevAssignment,
                                             ZookeeperState currentAssignment, Map<JobId, List<WorkItem>> itemsToAssign) {
        return availability;

    }
}
