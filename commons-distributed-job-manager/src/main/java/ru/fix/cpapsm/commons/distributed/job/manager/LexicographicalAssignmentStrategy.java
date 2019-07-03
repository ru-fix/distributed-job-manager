package ru.fix.cpapsm.commons.distributed.job.manager;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Ignore current workers assignment.
 * Assign jobs to workers and try balance loading uniformly
 *
 * @author Kamil Asfandiyarov
 */
class LexicographicalAssignmentStrategy implements AssignmentStrategy {

    @Override
    public Map<String, Set<String>> reassignAndBalance(
            Map<String, Set<String>> availableRepeatableJobs,
            Map<String, Set<String>> assignedRepeatableJobs) {

        Map<String, Set<String>> newAssignments = availableRepeatableJobs.keySet().stream()
                .collect(Collectors.toMap(key -> key, key -> new HashSet<>()));

        List<String> jobs = availableRepeatableJobs.values().stream()
                .flatMap(Set::stream)
                .distinct()
                .sorted()
                .collect(Collectors.toList());

        List<String> workers = assignedRepeatableJobs.keySet().stream()
                .distinct()
                .sorted()
                .collect(Collectors.toList());

        for (String job : jobs) {
            workers.stream()
                    .sorted()
                    .filter(w -> availableRepeatableJobs.get(w).contains(job))
                    .min((w1, w2) -> Integer.compare(newAssignments.get(w1).size(), newAssignments.get(w2).size()))
                    .ifPresent(worker -> newAssignments.get(worker).add(job));
        }

        return newAssignments;
    }
}
