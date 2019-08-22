package ru.fix.distributed.job.manager.strategy;


import org.junit.jupiter.api.Test;
import ru.fix.distributed.job.manager.model.distribution.JobState;
import ru.fix.distributed.job.manager.model.distribution.WorkPoolItem;
import ru.fix.distributed.job.manager.model.distribution.WorkerItem;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class DefaultAssignmentStrategyTest {

    static void populate(JobState jobState, String workerId, String... workPoolIds) {
        WorkerItem workerItem = new WorkerItem(workerId);
        Arrays.asList(workPoolIds).forEach(v -> workerItem.getWorkPools().add(new WorkPoolItem(v)));
        jobState.getWorkers().add(workerItem);
    }

    final DefaultAssignmentStrategy strategy = new DefaultAssignmentStrategy();

    @Test
    public void lexicographical_case() throws Exception {
        JobState available = new JobState();
        JobState current = new JobState();
        JobState expected = new JobState();

        populate(current, "w1", "1", "2");

        populate(available, "w1", "1", "2");
        populate(available, "w2", "1", "2");

        populate(expected, "w1", "1");
        populate(expected, "w2", "2");

        assertEquals(expected.toMap(), strategy.reassignAndBalance(available, current).toMap());
    }

    @Test
    public void sticky_workload_reverse_lexicographical() throws Exception {
        JobState available = new JobState();
        JobState current = new JobState();
        JobState expected = new JobState();

        populate(current, "w2", "2");
        populate(current, "w3", "1", "3");

        populate(available, "w2", "1", "2", "3");
        populate(available, "w3", "1", "2", "3");
        populate(available, "w1", "1", "3");

        populate(expected, "w2", "2");
        populate(expected, "w3", "1");
        populate(expected, "w1", "3");

        assertEquals(expected.toMap(), strategy.reassignAndBalance(available, current).toMap());
    }


    @Test
    public void init_balance() throws Exception {
        JobState available = new JobState();
        JobState current = new JobState();

        populate(available, "w1", "1", "2", "3", "4", "5");
        populate(available, "w2", "1", "2", "3", "4", "5");
        populate(available, "w3", "1", "2", "3", "4", "5");

        Map<WorkerItem, Set<WorkPoolItem>> assignmentMap = strategy.reassignAndBalance(available, current).toMap();

        assertTrue(assignmentMap.get(new WorkerItem("w1")).size() <= 2);
        assertTrue(assignmentMap.get(new WorkerItem("w2")).size() <= 2);
        assertTrue(assignmentMap.get(new WorkerItem("w3")).size() <= 2);

        assertEquals(5, assignmentMap.get(new WorkerItem("w1")).size() + assignmentMap.get(new
                WorkerItem("w2")).size() + assignmentMap.get(new WorkerItem("w3")).size(),
                "count of jobs");

        assertEquals(15, assignmentMap.values().stream()
                .flatMap(Collection::stream)
                .map(WorkPoolItem::getId)
                .map(Integer::parseInt)
                .reduce((a, b) -> a + b)
                .orElse(0)
                .intValue(),
                "sum of jobs");

    }

}
