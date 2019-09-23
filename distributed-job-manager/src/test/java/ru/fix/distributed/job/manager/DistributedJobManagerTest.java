package ru.fix.distributed.job.manager;

import org.apache.curator.framework.CuratorFramework;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import ru.fix.aggregating.profiler.AggregatingProfiler;
import ru.fix.distributed.job.manager.strategy.AssignmentStrategies;
import ru.fix.dynamic.property.api.DynamicProperty;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class DistributedJobManagerTest extends AbstractJobManagerTest {

//    @Disabled("The test successfully pass in single mode, but fails in build")
    @Test
    public void shouldEvenlyReassignWorkItemsForThreeWorkers() throws Exception {
        StubbedMultiJob job1 = new StubbedMultiJob(
                0, createWorkPool("distr-job-id-0", 1).getItems(), 50000L
        );
        StubbedMultiJob job2 = new StubbedMultiJob(
                1, createWorkPool("distr-job-id-1", 6).getItems(), 50000L
        );
        StubbedMultiJob job3 = new StubbedMultiJob(
                2, createWorkPool("distr-job-id-2", 2).getItems(), 50000L
        );
        CuratorFramework curator = zkTestingServer.createClient();

        DistributedJobManager djm = new DistributedJobManager(
                "worker-" + 0,
                curator,
                JOB_MANAGER_ZK_ROOT_PATH,
                Arrays.asList(job1, job2, job3),
                AssignmentStrategies.DEFAULT,
                new AggregatingProfiler(),
                DynamicProperty.of(10_000L),
                DynamicProperty.of(true)
        );

        DistributedJobManager djm1 = new DistributedJobManager(
                "worker-" + 1,
                zkTestingServer.createClient(),
                JOB_MANAGER_ZK_ROOT_PATH,
                Collections.emptyList(),
                AssignmentStrategies.DEFAULT,
                new AggregatingProfiler(),
                DynamicProperty.of(10_000L),
                DynamicProperty.of(false)
        );

        DistributedJobManager djm2 = new DistributedJobManager(
                "worker-" + 2,
                zkTestingServer.createClient(),
                JOB_MANAGER_ZK_ROOT_PATH,
                Collections.emptyList(),
                AssignmentStrategies.DEFAULT,
                new AggregatingProfiler(),
                DynamicProperty.of(10_000L),
                DynamicProperty.of(false)
        );
        Thread.sleep(2500);

        List<String> nodes = Arrays.asList(
                paths.getAssignedWorkItem("worker-2", "distr-job-id-1", "distr-job-id-1.work-item-1"),
                paths.getAssignedWorkItem("worker-2", "distr-job-id-1", "distr-job-id-1.work-item-3"),
                paths.getAssignedWorkItem("worker-2", "distr-job-id-1", "distr-job-id-1.work-item-5"),

                paths.getAssignedWorkItem("worker-1", "distr-job-id-0", "distr-job-id-0.work-item-0"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-2"),
                paths.getAssignedWorkItem("worker-1", "distr-job-id-1", "distr-job-id-1.work-item-4"),

                paths.getAssignedWorkItem("worker-0", "distr-job-id-2", "distr-job-id-2.work-item-0"),
                paths.getAssignedWorkItem("worker-0", "distr-job-id-2", "distr-job-id-2.work-item-1"),
                paths.getAssignedWorkItem("worker-0", "distr-job-id-1", "distr-job-id-1.work-item-0")
        );

        for (String node : nodes) {
            assertNotNull(curator.checkExists().forPath(node));
        }
    }

    private WorkPool createWorkPool(String jobId, int workItemsNumber) {
        Set<String> workPool = new HashSet<>();

        for (int i = 0; i < workItemsNumber; i++) {
            workPool.add(jobId + ".work-item-" + i);
        }

        return WorkPool.of(workPool);
    }
}