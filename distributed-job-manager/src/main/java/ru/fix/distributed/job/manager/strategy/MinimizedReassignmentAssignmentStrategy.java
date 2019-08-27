package ru.fix.distributed.job.manager.strategy;

import ru.fix.distributed.job.manager.model.distribution.JobState;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Assignment strategy that minimize job reassignment within
 * living workers when new worker added or one of workers removed
 *
 * @author Kamil Asfandiyarov
 */
public class MinimizedReassignmentAssignmentStrategy implements AssignmentStrategy {

    public Map<String, Set<String>> reassignAndBalance(Map<String, Set<String>> availableRepeatableJobs,
                                                       Map<String, Set<String>> currentAssignment) {

        Map<String, Set<String>> newAssigment = new HashMap<>();
        for (String worker : availableRepeatableJobs.keySet()) {
            newAssigment.put(worker, new HashSet<>());
        }

        /**
         * Freedom degree - how many workers can run job.
         * Map job to list of workers that can run job.
         */
        Map<String, Set<String>> jobFreedom = new HashMap<>();

        for (Map.Entry<String, Set<String>> availableJobs : availableRepeatableJobs.entrySet()) {
            for (String job : availableJobs.getValue()) {
                jobFreedom.computeIfAbsent(job, j -> new HashSet<>())
                        .add(availableJobs.getKey());
            }
        }

        /**
         * Order jobs from lower freedom degree to higher.
         */
        List<String> jobs = availableRepeatableJobs.values().stream()
                .flatMap(Collection::stream)
                .distinct()
                .sorted((j1, j2) -> {
                    int freedomComparison = Integer.compare(jobFreedom.get(j1).size(), jobFreedom.get(j2).size());
                    if (freedomComparison != 0) {
                        return freedomComparison;
                    } else {
                        return j1.compareTo(j2);
                    }
                })
                .collect(Collectors.toList());

        /**
         * Assign each job to worker.
         */
        for (String job : jobs) {
            Optional<String> worker = jobFreedom.get(job).stream()
                    .min((w1, w2) -> {
                        int workerWorkLoadComparison = Integer.compare(newAssigment.get(w1).size(),
                                newAssigment.get(w2).size());
                        if (workerWorkLoadComparison != 0) {
                            return workerWorkLoadComparison;
                        } else {
                            if (currentAssignment.getOrDefault(w1, Collections.emptySet()).contains(job)) {
                                return -1;
                            } else if (currentAssignment.getOrDefault(w2, Collections.emptySet()).contains(job)) {
                                return 1;
                            } else {
                                return w1.compareTo(w2);
                            }
                        }
                    });
            worker.ifPresent(w -> newAssigment.get(w).add(job));
        }

        return newAssigment;
    }

    @Override
    public JobState reassignAndBalance(JobState jobAvailability, JobState currentAssignment) {
        return null;
    }
}
