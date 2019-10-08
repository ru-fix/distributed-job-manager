package ru.fix.distributed.job.manager;

import org.apache.curator.framework.CuratorFramework;
import org.junit.jupiter.api.Test;
import ru.fix.aggregating.profiler.AggregatingProfiler;
import ru.fix.distributed.job.manager.model.AssignmentState;
import ru.fix.distributed.job.manager.model.JobId;
import ru.fix.distributed.job.manager.model.WorkItem;
import ru.fix.distributed.job.manager.model.WorkerId;
import ru.fix.distributed.job.manager.strategy.AbstractAssignmentStrategy;
import ru.fix.distributed.job.manager.strategy.AssignmentStrategies;
import ru.fix.distributed.job.manager.strategy.AssignmentStrategy;
import ru.fix.dynamic.property.api.DynamicProperty;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class DistributedJobManagerTest extends AbstractJobManagerTest {

    @Test
    void shouldEvenlyReassignWorkItemsForEachDjm() throws Exception {
        createDjmWithEvenlySpread("worker-0", Collections.singletonList(distributedJobs().get(0)));
        createDjmWithEvenlySpread("worker-1", Collections.singletonList(distributedJobs().get(1)));
        createDjmWithEvenlySpread("worker-2", Collections.singletonList(distributedJobs().get(2)));
        Thread.sleep(1000);

        List<String> nodes = Arrays.asList(
                paths.getAssignedWorkItem("worker-2", "distr-job-id-2", "distr-job-id-2.work-item-1"),
                paths.getAssignedWorkItem("worker-2", "distr-job-id-2", "distr-job-id-2.work-item-0"),

                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-1"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-2"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-3"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-4"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-5"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-0"),

                paths.getAssignedWorkItem("worker-0", "distr-job-id-0", "distr-job-id-0.work-item-0")
        );

        CuratorFramework curator = zkTestingServer.createClient();
        for (String node : nodes) {
            assertNotNull(curator.checkExists().forPath(node));
        }
    }

    @Test
    void shouldEvenlyReassignWorkItemsForThreeIdenticalWorkers() throws Exception {
        createDjmWithEvenlySpread("worker-0", distributedJobs());
        createDjmWithEvenlySpread("worker-1", distributedJobs());
        Thread.sleep(500);
        createDjmWithEvenlySpread("worker-2", distributedJobs());
        Thread.sleep(1500);

        List<String> nodes = Arrays.asList(
                paths.getAssignedWorkItem("worker-2", "distr-job-id-1", "distr-job-id-1.work-item-3"),
                paths.getAssignedWorkItem("worker-2", "distr-job-id-1", "distr-job-id-1.work-item-1"),
                paths.getAssignedWorkItem("worker-2", "distr-job-id-1", "distr-job-id-1.work-item-0"),

                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-2"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-2", "distr-job-id-2.work-item-1"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-5"),

                paths.getAssignedWorkItem("worker-0", "distr-job-id-1", "distr-job-id-1.work-item-4"),
                paths.getAssignedWorkItem("worker-0", "distr-job-id-0", "distr-job-id-0.work-item-0"),
                paths.getAssignedWorkItem("worker-0", "distr-job-id-2", "distr-job-id-2.work-item-0")
        );

        CuratorFramework curator = zkTestingServer.createClient();
        for (String node : nodes) {
            assertNotNull(curator.checkExists().forPath(node));
        }
    }

    @Test
    void shouldEvenlyReassignWorkItemsForEachDjmUsingRendezvous() throws Exception {
        createDjmWithRendezvous("worker-0", Collections.singletonList(distributedJobs().get(0)));
        createDjmWithRendezvous("worker-1", Collections.singletonList(distributedJobs().get(1)));
        createDjmWithRendezvous("worker-2", Collections.singletonList(distributedJobs().get(2)));
        Thread.sleep(1000);

        List<String> nodes = Arrays.asList(
                paths.getAssignedWorkItem("worker-2", "distr-job-id-2", "distr-job-id-2.work-item-1"),
                paths.getAssignedWorkItem("worker-2", "distr-job-id-2", "distr-job-id-2.work-item-0"),

                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-1"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-2"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-3"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-4"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-5"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-0"),

                paths.getAssignedWorkItem("worker-0", "distr-job-id-0", "distr-job-id-0.work-item-0")
        );

        CuratorFramework curator = zkTestingServer.createClient();
        for (String node : nodes) {
            assertNotNull(curator.checkExists().forPath(node));
        }
    }

    @Test
    void shouldEvenlyReassignWorkItemsForThreeIdenticalWorkersUsingRendezvous() throws Exception {
        createDjmWithRendezvous("worker-0", distributedJobs());
        createDjmWithRendezvous("worker-1", distributedJobs());
        createDjmWithRendezvous("worker-2", distributedJobs());
        Thread.sleep(1000);

        List<String> nodes = Arrays.asList(
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-0"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-0", "distr-job-id-0.work-item-0"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-2", "distr-job-id-2.work-item-1"),

                paths.getAssignedWorkItem("worker-0", "distr-job-id-1", "distr-job-id-1.work-item-5"),
                paths.getAssignedWorkItem("worker-0", "distr-job-id-1", "distr-job-id-1.work-item-2"),
                paths.getAssignedWorkItem("worker-0", "distr-job-id-1", "distr-job-id-1.work-item-3"),

                paths.getAssignedWorkItem("worker-2", "distr-job-id-2", "distr-job-id-2.work-item-0"),
                paths.getAssignedWorkItem("worker-2", "distr-job-id-1", "distr-job-id-1.work-item-4"),
                paths.getAssignedWorkItem("worker-2", "distr-job-id-1", "distr-job-id-1.work-item-1")
        );

        CuratorFramework curator = zkTestingServer.createClient();
        for (String node : nodes) {
            assertNotNull(curator.checkExists().forPath(node));
        }
    }

    @Test
    void shouldEvenlyReassignIfOneWorkerDestroyed() throws Exception {
        createDjmWithEvenlySpread("worker-0", distributedJobs());
        createDjmWithEvenlySpread("worker-1", distributedJobs());
        Thread.sleep(500);
        DistributedJobManager destroyed = createDjmWithEvenlySpread("worker-2", distributedJobs());
        Thread.sleep(1500);

        List<String> nodes = Arrays.asList(
                paths.getAssignedWorkItem("worker-2", "distr-job-id-1", "distr-job-id-1.work-item-3"),
                paths.getAssignedWorkItem("worker-2", "distr-job-id-1", "distr-job-id-1.work-item-1"),
                paths.getAssignedWorkItem("worker-2", "distr-job-id-1", "distr-job-id-1.work-item-0"),

                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-2"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-2", "distr-job-id-2.work-item-1"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-5"),

                paths.getAssignedWorkItem("worker-0", "distr-job-id-1", "distr-job-id-1.work-item-4"),
                paths.getAssignedWorkItem("worker-0", "distr-job-id-0", "distr-job-id-0.work-item-0"),
                paths.getAssignedWorkItem("worker-0", "distr-job-id-2", "distr-job-id-2.work-item-0")
        );

        CuratorFramework curator = zkTestingServer.createClient();
        for (String node : nodes) {
            assertNotNull(curator.checkExists().forPath(node));
        }

        destroyed.close();
        Thread.sleep(1000);

        List<String> nodesAfterDestroy = Arrays.asList(
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-3"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-2"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-0"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-2", "distr-job-id-2.work-item-1"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-5"),

                paths.getAssignedWorkItem("worker-0", "distr-job-id-1", "distr-job-id-1.work-item-4"),
                paths.getAssignedWorkItem("worker-0", "distr-job-id-1", "distr-job-id-1.work-item-1"),
                paths.getAssignedWorkItem("worker-0", "distr-job-id-0", "distr-job-id-0.work-item-0"),
                paths.getAssignedWorkItem("worker-0", "distr-job-id-2", "distr-job-id-2.work-item-0")
        );

        for (String node : nodesAfterDestroy) {
            assertNotNull(curator.checkExists().forPath(node));
        }
    }

    private AbstractAssignmentStrategy ussdAssignmentStrategy = new AbstractAssignmentStrategy() {

        @Override
        public AssignmentState reassignAndBalance(
                Map<JobId, Set<WorkerId>> availability,
                AssignmentState prevAssignment,
                AssignmentState currentAssignment,
                Set<WorkItem> itemsToAssign
        ) {
            for (Map.Entry<JobId, Set<WorkerId>> jobEntry : availability.entrySet()) {
                Set<WorkItem> itemsToAssignForJob = getWorkItemsByJob(jobEntry.getKey(), itemsToAssign);

                jobEntry.getValue().forEach(workerId -> currentAssignment.putIfAbsent(workerId, new HashSet<>()));

                for (WorkItem item : itemsToAssignForJob) {
                    if (prevAssignment.containsWorkItem(item)) {
                        WorkerId workerFromPrevious = prevAssignment.getWorkerOfWorkItem(item);
                        currentAssignment.addWorkItem(workerFromPrevious, item);
                    } else {
                        WorkerId lessBusyWorker = currentAssignment
                                .getLessBusyWorkerFromAvailableWorkers(jobEntry.getValue());
                        currentAssignment.addWorkItem(lessBusyWorker, item);
                    }
                }

            }
            return currentAssignment;
        }
    };

    // Strategy assign work items on workers, which doesn't contains of any work item of ussd job
    private AbstractAssignmentStrategy smsAssignmentStrategy = new AbstractAssignmentStrategy() {

        @Override
        public AssignmentState reassignAndBalance(
                Map<JobId, Set<WorkerId>> availability,
                AssignmentState prevAssignment,
                AssignmentState currentAssignment,
                Set<WorkItem> itemsToAssign
        ) {
            for (Map.Entry<JobId, Set<WorkerId>> jobEntry : availability.entrySet()) {
                Set<WorkItem> itemsToAssignForJob = getWorkItemsByJob(jobEntry.getKey(), itemsToAssign);
                Set<WorkerId> availableWorkers = new HashSet<>(jobEntry.getValue());

                jobEntry.getValue().forEach(workerId -> {
                    currentAssignment.putIfAbsent(workerId, new HashSet<>());

                    // ignore worker, where ussd job was launched
                    if (currentAssignment.containsAnyWorkItemOfJob(workerId, new JobId("distr-job-id-1"))) {
                        availableWorkers.remove(workerId);
                    }
                });

                for (WorkItem item : itemsToAssignForJob) {
                    if (currentAssignment.containsWorkItem(item)) {
                        continue;
                    }

                    WorkerId lessBusyWorker = currentAssignment
                            .getLessBusyWorkerFromAvailableWorkers(availableWorkers);
                    currentAssignment.addWorkItem(lessBusyWorker, item);
                    itemsToAssign.remove(item);
                }
            }
            return currentAssignment;
        }
    };

    @Test
    void shouldReassignJobUsingCustomAssignmentStrategy() throws Exception {
        StubbedMultiJob smsJob = new StubbedMultiJob(
                0, createWorkPool("distr-job-id-0", 3).getItems(), 50000L
        );
        StubbedMultiJob ussdJob = new StubbedMultiJob(
                1, createWorkPool("distr-job-id-1", 1).getItems(), 50000L
        );
        StubbedMultiJob rebillJob = new StubbedMultiJob(
                2, createWorkPool("distr-job-id-2", 7).getItems(), 50000L
        );

        AssignmentStrategy customStrategy = (availability, prevAssignment, currentAssignment, itemsToAssign) -> {
            AssignmentState newState = ussdAssignmentStrategy.reassignAndBalance(
                    Map.of(new JobId("distr-job-id-1"), availability.get(new JobId("distr-job-id-1"))),
                    prevAssignment,
                    currentAssignment,
                    itemsToAssign
            );
            availability.remove(new JobId("distr-job-id-1"));

            newState = smsAssignmentStrategy.reassignAndBalance(
                    Map.of(new JobId("distr-job-id-0"), availability.get(new JobId("distr-job-id-0"))),
                    prevAssignment,
                    newState,
                    itemsToAssign
            );
            availability.remove(new JobId("distr-job-id-0"));

            // reassign items of other jobs using evenly spread strategy
            return AssignmentStrategies.EVENLY_SPREAD.reassignAndBalance(
                    availability,
                    prevAssignment,
                    newState,
                    itemsToAssign
            );
        };

        createDjm("worker-0", Arrays.asList(smsJob, ussdJob, rebillJob), customStrategy);
        createDjm("worker-1", Arrays.asList(smsJob, ussdJob, rebillJob), customStrategy);
        createDjm("worker-2", Arrays.asList(smsJob, ussdJob, rebillJob), customStrategy);
        createDjm("worker-3", Arrays.asList(smsJob, ussdJob, rebillJob), customStrategy);
        Thread.sleep(1500);

        List<String> nodes = Arrays.asList(
                paths.getAssignedWorkItem("worker-3", "distr-job-id-0", "distr-job-id-0.work-item-2"),
                paths.getAssignedWorkItem("worker-3", "distr-job-id-2", "distr-job-id-2.work-item-0"),
                paths.getAssignedWorkItem("worker-3", "distr-job-id-2", "distr-job-id-2.work-item-1"),

                paths.getAssignedWorkItem("worker-2", "distr-job-id-2", "distr-job-id-2.work-item-2"),
                paths.getAssignedWorkItem("worker-2", "distr-job-id-2", "distr-job-id-2.work-item-3"),
                paths.getAssignedWorkItem("worker-2", "distr-job-id-0", "distr-job-id-0.work-item-1"),

                paths.getAssignedWorkItem("worker-1", "distr-job-id-2", "distr-job-id-2.work-item-5"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-2", "distr-job-id-2.work-item-6"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-0"),

                paths.getAssignedWorkItem("worker-0", "distr-job-id-0", "distr-job-id-0.work-item-0"),
                paths.getAssignedWorkItem("worker-0", "distr-job-id-2", "distr-job-id-2.work-item-4")
        );

        CuratorFramework curator = zkTestingServer.createClient();
        for (String node : nodes) {
            assertNotNull(curator.checkExists().forPath(node));
        }
    }

    private List<DistributedJob> distributedJobs() {
        return Arrays.asList(
                new StubbedMultiJob(
                        0, createWorkPool("distr-job-id-0", 1).getItems(), 50000L
                ),
                new StubbedMultiJob(
                        1, createWorkPool("distr-job-id-1", 6).getItems(), 50000L
                ),
                new StubbedMultiJob(
                        2, createWorkPool("distr-job-id-2", 2).getItems(), 50000L
                ));
    }

    private DistributedJobManager createDjm(
            String nodeId,
            List<DistributedJob> jobs,
            AssignmentStrategy strategy
    ) throws Exception {
        return new DistributedJobManager(
                nodeId,
                zkTestingServer.createClient(),
                JOB_MANAGER_ZK_ROOT_PATH,
                jobs,
                strategy,
                new AggregatingProfiler(),
                DynamicProperty.of(10_000L)
        );
    }

    private DistributedJobManager createDjmWithEvenlySpread(String nodeId, List<DistributedJob> jobs) throws Exception {
        return createDjm(nodeId, jobs, AssignmentStrategies.EVENLY_SPREAD);
    }

    private DistributedJobManager createDjmWithRendezvous(String nodeId, List<DistributedJob> jobs) throws Exception {
        return createDjm(nodeId, jobs, AssignmentStrategies.RENDEZVOUS);
    }

    private WorkPool createWorkPool(String jobId, int workItemsNumber) {
        return WorkPool.of(IntStream.range(0, workItemsNumber)
                .mapToObj(i -> jobId + ".work-item-" + i)
                .collect(Collectors.toSet()));
    }
}