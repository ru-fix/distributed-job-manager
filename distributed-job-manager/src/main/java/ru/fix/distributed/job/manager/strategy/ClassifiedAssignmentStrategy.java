package ru.fix.distributed.job.manager.strategy;

import ru.fix.distributed.job.manager.model.distribution.JobState;
import ru.fix.distributed.job.manager.model.distribution.WorkPoolItem;
import ru.fix.distributed.job.manager.model.distribution.WorkerItem;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Assignment strategy for classified (grouped by type) jobs,
 * so one worker cannot handle more than one job of the same class.
 * <p>
 * The strategy needs for external classifier to split the jobs into the classes.
 * <p>
 * Classifier is Function that maps provided job (WorkPoolItem) to String-value class id.
 * <p>
 * Example:
 * <p>
 * workers: w1, w2 (which can handle all type of jobs), w3 (can handle only 'b'-jobs)
 * <p>
 * jobs: a1, a2, a3 (grouped as 'a'-jobs); b1, b2, b3, b4, b5 (grouped as 'b'-jobs): c1 ('c'-jobs)
 * <p>
 * It can be such assignment:
 * <p>
 * w1: a1, b2
 * <p>
 * w2: a3, b5, c1
 * <p>
 * w3: b1
 * <p>
 * unassigned jobs: a2, b3, b4 (there are not enough workers to handle this jobs)
 */
public class ClassifiedAssignmentStrategy implements AssignmentStrategy {

    private final Function<WorkPoolItem, String> classifier;

    /**
     * Construct assignment strategy with provided classifier
     */
    public ClassifiedAssignmentStrategy(Function<WorkPoolItem, String> classifier) {
        this.classifier = classifier;
    }

    @Override
    public JobState reassignAndBalance(JobState jobAvailability, JobState currentAssignment) {
        Map<WorkerItem, HashSet<WorkPoolItem>> newAssignments = jobAvailability.getWorkers().stream()
                .collect(Collectors.toMap(key -> key, key -> new HashSet<>()));

        List<WorkPoolItem> currJobs = currentAssignment.getWorkers().stream()
                .flatMap(i -> i.getWorkPools().stream())
                .collect(Collectors.toList());

        Map<WorkPoolItem, WorkerItem> currJobsToWorker = new HashMap<>();
        currentAssignment.getWorkers().forEach(workerItem -> workerItem.getWorkPools()
                .forEach(workPoolItem -> currJobsToWorker.put(workPoolItem, workerItem)));

        List<WorkPoolItem> jobs = jobAvailability.getWorkers().stream()
                .flatMap(i -> i.getWorkPools().stream())
                .distinct()
                .sorted((o1, o2) -> {
                    boolean o1Existed = currJobs.contains(o1);
                    boolean o2Existed = currJobs.contains(o2);
                    if (o1Existed != o2Existed) {
                        return o1Existed ? -1 : 1;
                    }
                    WorkerItem prevO1Worker = currJobsToWorker.get(o1);
                    if (prevO1Worker != null && !jobAvailability.getWorkers().contains(prevO1Worker)) {
                        return 1;
                    }
                    WorkerItem prevO2Worker = currJobsToWorker.get(o2);
                    if (prevO2Worker != null && !jobAvailability.getWorkers().contains(prevO2Worker)) {
                        return -1;
                    }
                    return o1.getId().compareTo(o2.getId());
                })
                .collect(Collectors.toList());

        Map<String, List<WorkPoolItem>> jobsByGroup = jobs.stream()
                .collect(Collectors.groupingBy(classifier));

        Map<String, List<WorkerItem>> workersByGroup = new HashMap<>();
        jobAvailability.getWorkers().forEach(worker -> worker.getWorkPools().stream()
                .map(classifier)
                .distinct()
                .forEach(gr -> workersByGroup.computeIfAbsent(gr, s -> new ArrayList<>()).add(worker)));

        for (Iterator<Map.Entry<String, List<WorkPoolItem>>> iterator = jobsByGroup.entrySet().iterator(); iterator
                .hasNext(); ) {
            Map.Entry<String, List<WorkPoolItem>> entry = iterator.next();

            String group = entry.getKey();
            List<WorkPoolItem> jobsOfGroup = entry.getValue();

            List<WorkerItem> workersOfGroup = workersByGroup.get(group);

            // task assignment, where task count is greater or equal to worker count
            if (jobsOfGroup.size() >= workersOfGroup.size()) {
                for (Iterator<WorkerItem> workerItemIterator = workersOfGroup.iterator();
                     workerItemIterator.hasNext(); ) {
                    WorkerItem workerItem = workerItemIterator.next();
                    WorkPoolItem workPoolItem = null;

                    // trying to keep assignment
                    Optional<WorkerItem> prevWorkerItem = currentAssignment.getWorkers().stream()
                            .filter(w -> w.equals(workerItem)).findAny();
                    boolean assigned = false;
                    if (prevWorkerItem.isPresent()) {
                        WorkerItem wi = prevWorkerItem.get();

                        Set<WorkPoolItem> set = new HashSet<>(wi.getWorkPools());
                        set.retainAll(jobsOfGroup);
                        if (!set.isEmpty()) {
                            // common workPoolItem found, keep the assignment
                            workPoolItem = set.iterator().next();
                            jobsOfGroup.remove(workPoolItem);
                            assigned = true;
                        }
                    }

                    if (!assigned) {
                        // no previous assignment found, try getNext first previously unassigned job
                        Optional<WorkPoolItem> any = jobsOfGroup.stream()
                                .filter(i -> currJobsToWorker.get(i) == null)
                                .findAny();
                        if (any.isPresent()) {
                            workPoolItem = any.get();
                            jobsOfGroup.remove(workPoolItem);
                        } else {
                            workPoolItem = jobsOfGroup.remove(0);
                        }
                    }

                    newAssignments.get(workerItem).add(workPoolItem);
                    jobs.remove(workPoolItem);
                    workerItemIterator.remove();
                }
                iterator.remove();
            }
        }

        // left tasks assignment
        for (WorkPoolItem job : jobs) {
            String group = classifier.apply(job);

            List<WorkerItem> workersOfGroup = workersByGroup.get(group);
            Optional<WorkerItem> preferredWorker = workersOfGroup.stream()
                    .min((o1, o2) -> {
                        int compare = Integer.compare(newAssignments.get(o1).size(), newAssignments.get(o2).size());
                        if (compare == 0) {
                            compare = Integer.compare(o1.getWorkPools().size(), o2.getWorkPools().size());
                        }
                        if (compare == 0) {
                            if (o1.equals(currJobsToWorker.get(job))) {
                                return -1;
                            }
                            if (o2.equals(currJobsToWorker.get(job))) {
                                return 1;
                            }
                        }
                        return compare;
                    });
            preferredWorker.ifPresent(workerItem -> {
                newAssignments.get(workerItem).add(job);
                workersOfGroup.remove(workerItem);
            });
        }

        // copy original items into new assignment state
        JobState newAssignmentState = new JobState();
        newAssignments.forEach((origWorkerItem, origWorkPoolItems) -> {
            WorkerItem workerItem = new WorkerItem(origWorkerItem.getId());
            newAssignmentState.getWorkers().add(workerItem);

            origWorkPoolItems.forEach(origWorkPoolItem -> workerItem.getWorkPools().add(new WorkPoolItem
                    (origWorkPoolItem.getId())));
        });

        return newAssignmentState;
    }

}
