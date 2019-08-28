package ru.fix.distributed.job.manager.strategy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import ru.fix.distributed.job.manager.model.distribution.JobState;
import ru.fix.distributed.job.manager.model.distribution.WorkPoolItem;
import ru.fix.distributed.job.manager.model.distribution.WorkerItem;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class ClassifiedAssignmentStrategyTest {

    private final AssignmentStrategy strategy = new ClassifiedAssignmentStrategy(
            // first letter classifier
            workPoolItem -> workPoolItem.getId().substring(0, 1)
    );

    private final AssignmentStrategy strategy2 = new ClassifiedAssignmentStrategy(
            new Function<>() {
                private ObjectMapper mapper = new ObjectMapper();

                @Override
                public String apply(WorkPoolItem workPoolItem) {
                    try {
                        return mapper.readTree(workPoolItem.getId()).get("smscServerId").asText();
                    } catch (IOException e) {
                        // cannot parse
                        Assertions.fail("Cannot extract 'region' field from sms sending jobId: " + workPoolItem.getId());
                    }
                    return workPoolItem.getId();
                }
            }
    );

    private static void populate(JobState jobState, String workerId, String... workPoolIds) {
        WorkerItem workerItem = new WorkerItem(workerId);
        Arrays.asList(workPoolIds).forEach(v -> workerItem.getWorkPools().add(new WorkPoolItem(v)));
        jobState.getWorkers().add(workerItem);
    }

    @Test
    public void init_balance() throws Exception {
        JobState available = new JobState();
        JobState current = new JobState();

        populate(available, "w1", "m1", "m2", "m3", "k1", "k2", "v1", "v2");
        populate(available, "w2", "m1", "m2", "m3", "k1", "k2", "v1", "v2");
        populate(available, "w3", "m1", "m2", "m3", "k1", "k2", "v1", "v2");

        Map<WorkerItem, Set<WorkPoolItem>> assignmentMap = strategy.reassignAndBalance(available, current).toMap();

        assertTrue(assignmentMap.get(new WorkerItem("w1")).size() <= 3);
        assertTrue(assignmentMap.get(new WorkerItem("w2")).size() <= 3);
        assertTrue(assignmentMap.get(new WorkerItem("w3")).size() <= 3);

        assertEquals(7, assignmentMap.get(new WorkerItem("w1")).size() +
                assignmentMap.get(new WorkerItem("w2")).size() +
                assignmentMap.get(new WorkerItem("w3")).size(), "count of jobs");
    }

    @Test
    public void init_balance2() throws Exception {
        JobState available = new JobState();
        JobState current = new JobState();

        populate(available, "w1", "m1", "m2", "m3", "k1", "k2", "v1", "v2");
        populate(available, "w2", "k1", "k2", "v1", "v2");
        populate(available, "w3", "v1", "v2");

        populate(current, "w1", "m2", "k2");
        populate(current, "w2", "k1", "v1");
        populate(current, "w3", "v2");

        Map<WorkerItem, Set<WorkPoolItem>> assignmentMap = strategy.reassignAndBalance(available, current).toMap();

        assertTrue(assignmentMap.get(new WorkerItem("w1")).size() <= 3);
        assertTrue(assignmentMap.get(new WorkerItem("w2")).size() <= 2);
        assertTrue(assignmentMap.get(new WorkerItem("w3")).size() <= 1);

        assertEquals(5, assignmentMap.get(new WorkerItem("w1")).size() +
                assignmentMap.get(new WorkerItem("w2")).size() +
                assignmentMap.get(new WorkerItem("w3")).size(),
                "count of jobs");
    }

    @Test
    public void init_balance3() throws Exception {
        JobState available = new JobState();
        JobState current = new JobState();

        populate(available, "w1", "m", "k", "v");
        populate(available, "w2", "k", "v");
        populate(available, "w3", "v");

        Map<WorkerItem, Set<WorkPoolItem>> assignmentMap = strategy.reassignAndBalance(available, current).toMap();

        assertEquals(1, assignmentMap.get(new WorkerItem("w1")).size());
        assertEquals(1, assignmentMap.get(new WorkerItem("w2")).size());
        assertEquals(1, assignmentMap.get(new WorkerItem("w3")).size());

        assertEquals(3, assignmentMap.get(new WorkerItem("w1")).size() +
                assignmentMap.get(new WorkerItem("w2")).size() +
                assignmentMap.get(new WorkerItem("w3")).size(),
                "count of jobs");
    }

    @Test
    public void init_balance4() throws Exception {
        JobState available = new JobState();
        JobState current = new JobState();

        populate(available, "w1",
                "m1", "m2", "m3", "m4", "m5",
                "k1", "k2", "k3", "k4", "k5",
                "v1", "v2", "v3", "v4", "v5");
        populate(available, "w2",
                "m1", "m2", "m3", "m4", "m5",
                "k1", "k2", "k3", "k4", "k5",
                "v1", "v2", "v3", "v4", "v5");
        populate(available, "w3",
                "m1", "m2", "m3", "m4", "m5",
                "k1", "k2", "k3", "k4", "k5",
                "v1", "v2", "v3", "v4", "v5");

        Map<WorkerItem, Set<WorkPoolItem>> assignmentMap = strategy.reassignAndBalance(available, current).toMap();

        assertEquals(3, assignmentMap.get(new WorkerItem("w1")).size());
        assertEquals(3, assignmentMap.get(new WorkerItem("w2")).size());
        assertEquals(3, assignmentMap.get(new WorkerItem("w3")).size());
    }

    @Test
    public void initBalanceSticky() throws Exception {
        JobState available = new JobState();
        JobState current = new JobState();

        populate(available, "w1",
                "m1", "m2", "m3", "m4", "m5",
                "k1", "k2", "k3", "k4", "k5",
                "v1", "v2", "v3", "v4", "v5");
        populate(available, "w2",
                "m1", "m2", "m3", "m4", "m5",
                "k1", "k2", "k3", "k4", "k5",
                "v1", "v2", "v3", "v4", "v5");
        populate(available, "w3",
                "m1", "m2", "m3", "m4", "m5",
                "k1", "k2", "k3", "k4", "k5",
                "v1", "v2", "v3", "v4", "v5");

        populate(current, "w1", "m1", "v1");
        populate(current, "w2", "m3", "k4", "v5");
        populate(current, "w3", "k3");

        Map<WorkerItem, Set<WorkPoolItem>> assignmentMap = strategy.reassignAndBalance(available, current).toMap();

        assertEquals(3, assignmentMap.get(new WorkerItem("w1")).size());
        assertEquals(3, assignmentMap.get(new WorkerItem("w2")).size());
        assertEquals(3, assignmentMap.get(new WorkerItem("w3")).size());

        assertTrue(assignmentMap.get(new WorkerItem("w1")).stream().map(WorkPoolItem::getId)
                .collect(Collectors.toList()).containsAll(Arrays.asList("m1", "v1")));
        assertTrue(assignmentMap.get(new WorkerItem("w2")).stream().map(WorkPoolItem::getId)
                .collect(Collectors.toList()).containsAll(Arrays.asList("m3", "k4", "v5")));
        assertTrue(assignmentMap.get(new WorkerItem("w3")).stream().map(WorkPoolItem::getId)
                .collect(Collectors.toList()).containsAll(Arrays.asList("k3")));
    }

    /**
     * Тест для доработки алгоритма минимального переназначения задач
     * <p>
     * current assignment:
     * w1: m1, v1
     * w2: m2, k2, v2
     * w3: k1
     * <p>
     * оптимальный reassignment:
     * w1: m1, v1
     * w2: k2, (m2 или v2)
     * w3: k1, (m2 или v2)
     * здесь перемещается только одна job'а (v2 или m2) с w2 на w3
     * <p>
     * фактический reassignment:
     * w1: k1, v1
     * w2: m2, v2
     * w3: m1, k2
     * здесь перемещаются 3 job'ы (k1, m1, k2)
     */
    @Test
    public void initBalance5() throws Exception {
        JobState available = new JobState();
        JobState current = new JobState();

        populate(available, "w1",
                "m1", "m2",
                "k1", "k2",
                "v1", "v2");
        populate(available, "w2",
                "m1", "m2",
                "k1", "k2",
                "v1", "v2");
        populate(available, "w3",
                "m1", "m2",
                "k1", "k2",
                "v1", "v2");

        populate(current, "w1", "m1", "v1");
        populate(current, "w2", "m2", "k2", "v2");
        populate(current, "w3", "k1");

        Map<WorkerItem, Set<WorkPoolItem>> assignmentMap = strategy.reassignAndBalance(available, current).toMap();

        assertEquals(2, assignmentMap.get(new WorkerItem("w1")).size());
        assertEquals(2, assignmentMap.get(new WorkerItem("w2")).size());
        assertEquals(2, assignmentMap.get(new WorkerItem("w3")).size());

        assertTrue(assignmentMap.get(new WorkerItem("w1")).stream().map(WorkPoolItem::getId)
                .collect(Collectors.toList()).containsAll(Arrays.asList("m1", "v1")));
        assertTrue(assignmentMap.get(new WorkerItem("w2")).stream().map(WorkPoolItem::getId)
                .collect(Collectors.toList()).containsAll(Arrays.asList("k2")));
        assertTrue(assignmentMap.get(new WorkerItem("w3")).stream().map(WorkPoolItem::getId)
                .collect(Collectors.toList()).containsAll(Arrays.asList("k1")));
    }

    @Test
    public void minimumReassignAfterDeletingWorker() throws Exception {
        JobState available = new JobState();
        JobState current = new JobState();

        populate(available, "w1",
                "m1", "m2", "k1", "k2", "k3");
        populate(available, "w2",
                "m1", "m2", "k1", "k2", "k3");
        populate(available, "w3",
                "m1", "m2", "k1", "k2", "k3");
        // w4 removed in this test
        populate(available, "w5",
                "m1", "m2", "k1", "k2", "k3");
        populate(available, "w6",
                "m1", "m2", "k1", "k2", "k3");

        populate(current, "w1", "m1");
        populate(current, "w2", "m2");
        populate(current, "w3", "k1");
        populate(current, "w4", "k2");
        populate(current, "w5", "k3");
        populate(current, "w6");

        Map<WorkerItem, Set<WorkPoolItem>> assignmentMap = strategy.reassignAndBalance(available, current).toMap();

        assertEquals(1, assignmentMap.get(new WorkerItem("w1")).size());
        assertEquals(1, assignmentMap.get(new WorkerItem("w2")).size());
        assertEquals(1, assignmentMap.get(new WorkerItem("w3")).size());
        assertEquals(1, assignmentMap.get(new WorkerItem("w5")).size());
        assertEquals(1, assignmentMap.get(new WorkerItem("w6")).size());

        assertTrue(assignmentMap.get(new WorkerItem("w1")).stream().map(WorkPoolItem::getId)
                .collect(Collectors.toList()).containsAll(Arrays.asList("m1")));
        assertTrue(assignmentMap.get(new WorkerItem("w2")).stream().map(WorkPoolItem::getId)
                .collect(Collectors.toList()).containsAll(Arrays.asList("m2")));
        assertTrue(assignmentMap.get(new WorkerItem("w3")).stream().map(WorkPoolItem::getId)
                .collect(Collectors.toList()).containsAll(Arrays.asList("k1")));
        assertTrue(assignmentMap.get(new WorkerItem("w5")).stream().map(WorkPoolItem::getId)
                .collect(Collectors.toList()).containsAll(Arrays.asList("k3")));
        assertTrue(assignmentMap.get(new WorkerItem("w6")).stream().map(WorkPoolItem::getId)
                .collect(Collectors.toList()).containsAll(Arrays.asList("k2")));
    }

    @Test
    public void minimumReassignAfterAddingWorker() throws Exception {
        JobState available = new JobState();
        JobState current = new JobState();

        populate(available, "w1",
                "m1", "m2", "k1", "k2", "k3");
        populate(available, "w2",
                "m1", "m2", "k1", "k2", "k3");
        populate(available, "w3",
                "m1", "m2", "k1", "k2", "k3");
        populate(available, "w4",
                "m1", "m2", "k1", "k2", "k3");
        populate(available, "w5",
                "m1", "m2", "k1", "k2", "k3");

        populate(current, "w1", "m1");
        populate(current, "w2", "m2");
        populate(current, "w3", "k1");
        populate(current, "w4", "k2");

        Map<WorkerItem, Set<WorkPoolItem>> assignmentMap = strategy.reassignAndBalance(available, current).toMap();

        assertEquals(1, assignmentMap.get(new WorkerItem("w1")).size());
        assertEquals(1, assignmentMap.get(new WorkerItem("w2")).size());
        assertEquals(1, assignmentMap.get(new WorkerItem("w3")).size());
        assertEquals(1, assignmentMap.get(new WorkerItem("w4")).size());
        assertEquals(1, assignmentMap.get(new WorkerItem("w5")).size());

        assertTrue(assignmentMap.get(new WorkerItem("w1")).stream().map(WorkPoolItem::getId)
                .collect(Collectors.toList()).containsAll(Collections.singletonList("m1")));
        assertTrue(assignmentMap.get(new WorkerItem("w2")).stream().map(WorkPoolItem::getId)
                .collect(Collectors.toList()).containsAll(Arrays.asList("m2")));
        assertTrue(assignmentMap.get(new WorkerItem("w3")).stream().map(WorkPoolItem::getId)
                .collect(Collectors.toList()).containsAll(Arrays.asList("k1")));
        assertTrue(assignmentMap.get(new WorkerItem("w4")).stream().map(WorkPoolItem::getId)
                .collect(Collectors.toList()).containsAll(Arrays.asList("k2")));
        assertTrue(assignmentMap.get(new WorkerItem("w5")).stream().map(WorkPoolItem::getId)
                .collect(Collectors.toList()).containsAll(Arrays.asList("k3")));
    }

    @Test
    public void reassignTest() throws Exception {
        String[] workPool = {
                "{\"smscServerId\":1,\"smppServerAddress\":{\"host\":\"smsc-1\",\"port\":5019}," +
                        "\"credentials\":{\"index\":0,\"login\":\"login_1\",\"password\":\"pass_1\"}}",
                "{\"smscServerId\":1,\"smppServerAddress\":{\"host\":\"smsc-1\",\"port\":5019}," +
                        "\"credentials\":{\"index\":1,\"login\":\"login_1\",\"password\":\"pass_1\"}}",
                "{\"smscServerId\":1,\"smppServerAddress\":{\"host\":\"smsc-1\",\"port\":5019}," +
                        "\"credentials\":{\"index\":2,\"login\":\"login_1\",\"password\":\"pass_1\"}}",
                "{\"smscServerId\":1,\"smppServerAddress\":{\"host\":\"smsc-1\",\"port\":5019}," +
                        "\"credentials\":{\"index\":3,\"login\":\"login_1\",\"password\":\"pass_1\"}}",
                "{\"smscServerId\":2,\"smppServerAddress\":{\"host\":\"smsc-2\",\"port\":5019}," +
                        "\"credentials\":{\"index\":0,\"login\":\"login_2\",\"password\":\"pass_2\"}}",
                "{\"smscServerId\":2,\"smppServerAddress\":{\"host\":\"smsc-2\",\"port\":5019}," +
                        "\"credentials\":{\"index\":1,\"login\":\"login_2\",\"password\":\"pass_2\"}}",
                "{\"smscServerId\":2,\"smppServerAddress\":{\"host\":\"smsc-2\",\"port\":5019}," +
                        "\"credentials\":{\"index\":2,\"login\":\"login_2\",\"password\":\"pass_2\"}}",
                "{\"smscServerId\":2,\"smppServerAddress\":{\"host\":\"smsc-2\",\"port\":5019}," +
                        "\"credentials\":{\"index\":3,\"login\":\"login_2\",\"password\":\"pass_2\"}}",
                "{\"smscServerId\":3,\"smppServerAddress\":{\"host\":\"smsc-3\",\"port\":5019}," +
                        "\"credentials\":{\"index\":0,\"login\":\"login_3\",\"password\":\"pass_3\"}}",
                "{\"smscServerId\":3,\"smppServerAddress\":{\"host\":\"smsc-3\",\"port\":5019}," +
                        "\"credentials\":{\"index\":1,\"login\":\"login_3\",\"password\":\"pass_3\"}}",
                "{\"smscServerId\":3,\"smppServerAddress\":{\"host\":\"smsc-3\",\"port\":5019}," +
                        "\"credentials\":{\"index\":2,\"login\":\"login_3\",\"password\":\"pass_3\"}}",
                "{\"smscServerId\":3,\"smppServerAddress\":{\"host\":\"smsc-3\",\"port\":5019}," +
                        "\"credentials\":{\"index\":3,\"login\":\"login_3\",\"password\":\"pass_3\"}}",
                "{\"smscServerId\":4,\"smppServerAddress\":{\"host\":\"smsc-4\",\"port\":5019}," +
                        "\"credentials\":{\"index\":0,\"login\":\"login_4\",\"password\":\"pass_4\"}}",
                "{\"smscServerId\":4,\"smppServerAddress\":{\"host\":\"smsc-4\",\"port\":5019}," +
                        "\"credentials\":{\"index\":1,\"login\":\"login_4\",\"password\":\"pass_4\"}}",
                "{\"smscServerId\":4,\"smppServerAddress\":{\"host\":\"smsc-4\",\"port\":5019}," +
                        "\"credentials\":{\"index\":2,\"login\":\"login_4\",\"password\":\"pass_4\"}}",
                "{\"smscServerId\":4,\"smppServerAddress\":{\"host\":\"smsc-4\",\"port\":5019}," +
                        "\"credentials\":{\"index\":3,\"login\":\"login_4\",\"password\":\"pass_4\"}}",
                "{\"smscServerId\":5,\"smppServerAddress\":{\"host\":\"smsc-5\",\"port\":5019}," +
                        "\"credentials\":{\"index\":0,\"login\":\"login_5\",\"password\":\"pass_5\"}}",
                "{\"smscServerId\":5,\"smppServerAddress\":{\"host\":\"smsc-5\",\"port\":5019}," +
                        "\"credentials\":{\"index\":1,\"login\":\"login_5\",\"password\":\"pass_5\"}}",
                "{\"smscServerId\":5,\"smppServerAddress\":{\"host\":\"smsc-5\",\"port\":5019}," +
                        "\"credentials\":{\"index\":2,\"login\":\"login_5\",\"password\":\"pass_5\"}}",
                "{\"smscServerId\":5,\"smppServerAddress\":{\"host\":\"smsc-5\",\"port\":5019}," +
                        "\"credentials\":{\"index\":3,\"login\":\"login_5\",\"password\":\"pass_5\"}}",
                "{\"smscServerId\":6,\"smppServerAddress\":{\"host\":\"smsc-6\",\"port\":5019}," +
                        "\"credentials\":{\"index\":0,\"login\":\"login_6\",\"password\":\"pass_6\"}}",
                "{\"smscServerId\":6,\"smppServerAddress\":{\"host\":\"smsc-6\",\"port\":5019}," +
                        "\"credentials\":{\"index\":1,\"login\":\"login_6\",\"password\":\"pass_6\"}}",
                "{\"smscServerId\":6,\"smppServerAddress\":{\"host\":\"smsc-6\",\"port\":5019}," +
                        "\"credentials\":{\"index\":2,\"login\":\"login_6\",\"password\":\"pass_6\"}}",
                "{\"smscServerId\":6,\"smppServerAddress\":{\"host\":\"smsc-6\",\"port\":5019}," +
                        "\"credentials\":{\"index\":3,\"login\":\"login_6\",\"password\":\"pass_6\"}}",
                "{\"smscServerId\":7,\"smppServerAddress\":{\"host\":\"smsc-7\",\"port\":5019}," +
                        "\"credentials\":{\"index\":0,\"login\":\"login_7\",\"password\":\"pass_7\"}}",
                "{\"smscServerId\":7,\"smppServerAddress\":{\"host\":\"smsc-7\",\"port\":5019}," +
                        "\"credentials\":{\"index\":1,\"login\":\"login_7\",\"password\":\"pass_7\"}}",
                "{\"smscServerId\":7,\"smppServerAddress\":{\"host\":\"smsc-7\",\"port\":5019}," +
                        "\"credentials\":{\"index\":2,\"login\":\"login_7\",\"password\":\"pass_7\"}}",
                "{\"smscServerId\":7,\"smppServerAddress\":{\"host\":\"smsc-7\",\"port\":5019}," +
                        "\"credentials\":{\"index\":3,\"login\":\"login_7\",\"password\":\"pass_7\"}}",
                "{\"smscServerId\":8,\"smppServerAddress\":{\"host\":\"smsc-8\",\"port\":5019}," +
                        "\"credentials\":{\"index\":0,\"login\":\"login_8\",\"password\":\"pass_8\"}}",
                "{\"smscServerId\":8,\"smppServerAddress\":{\"host\":\"smsc-8\",\"port\":5019}," +
                        "\"credentials\":{\"index\":1,\"login\":\"login_8\",\"password\":\"pass_8\"}}",
                "{\"smscServerId\":8,\"smppServerAddress\":{\"host\":\"smsc-8\",\"port\":5019}," +
                        "\"credentials\":{\"index\":2,\"login\":\"login_8\",\"password\":\"pass_8\"}}",
                "{\"smscServerId\":8,\"smppServerAddress\":{\"host\":\"smsc-8\",\"port\":5019}," +
                        "\"credentials\":{\"index\":3,\"login\":\"login_8\",\"password\":\"pass_8\"}}"};

        JobState available1 = new JobState();
        populate(available1, "w1", workPool);

        JobState available2 = new JobState();
        populate(available2, "w1", workPool);
        populate(available2, "w2", workPool);

        JobState available3 = new JobState();
        populate(available3, "w1", workPool);
        populate(available3, "w2", workPool);
        populate(available3, "w3", workPool);

        JobState available4 = new JobState();
        populate(available4, "w1", workPool);
        populate(available4, "w2", workPool);
        populate(available4, "w3", workPool);
        populate(available4, "w4", workPool);

        JobState available5 = new JobState();
        populate(available5, "w1", workPool);
        populate(available5, "w2", workPool);
        populate(available5, "w3", workPool);
        populate(available5, "w4", workPool);
        populate(available5, "w5", workPool);

        JobState available6 = new JobState();
        populate(available6, "w1", workPool);
        populate(available6, "w2", workPool);
        populate(available6, "w3", workPool);
        populate(available6, "w4", workPool);
        populate(available6, "w5", workPool);
        populate(available6, "w6", workPool);

        JobState available7 = new JobState();
        populate(available7, "w1", workPool);
        populate(available7, "w2", workPool);
        populate(available7, "w3", workPool);
        populate(available7, "w4", workPool);
        populate(available7, "w5", workPool);
        populate(available7, "w6", workPool);
        populate(available7, "w7", workPool);

        JobState available8 = new JobState();
        populate(available8, "w1", workPool);
        populate(available8, "w2", workPool);
        populate(available8, "w3", workPool);
        populate(available8, "w4", workPool);
        populate(available8, "w5", workPool);
        populate(available8, "w6", workPool);
        populate(available8, "w7", workPool);
        populate(available8, "w8", workPool);

        JobState jobState = new JobState();
        Map<WorkerItem, Set<WorkPoolItem>> assignmentMap;

        jobState = strategy2.reassignAndBalance(available4, jobState);
        assignmentMap = jobState.toMap();
        assignmentMap.forEach((workerItem, workPoolItems) -> assertEquals(8, workPoolItems.size()));
        assertEquals(4, assignmentMap.size());
        assertEquals(32, assignmentMap.values().stream().mapToLong(Collection::size).sum());

        jobState = strategy2.reassignAndBalance(available3, jobState);
        assignmentMap = jobState.toMap();
        assignmentMap.forEach((workerItem, workPoolItems) -> assertEquals(8, workPoolItems.size()));
        assertEquals(3, assignmentMap.size());
        assertEquals(24, assignmentMap.values().stream().mapToLong(Collection::size).sum());

        jobState = strategy2.reassignAndBalance(available2, jobState);
        assignmentMap = jobState.toMap();
        assignmentMap.forEach((workerItem, workPoolItems) -> assertEquals(8, workPoolItems.size()));
        assertEquals(2, assignmentMap.size());
        assertEquals(16, assignmentMap.values().stream().mapToLong(Collection::size).sum());

        jobState = strategy2.reassignAndBalance(available1, jobState);
        assignmentMap = jobState.toMap();
        assignmentMap.forEach((workerItem, workPoolItems) -> assertEquals(8, workPoolItems.size()));
        assertEquals(1, assignmentMap.size());
        assertEquals(8, assignmentMap.values().stream().mapToLong(Collection::size).sum());

        jobState = strategy2.reassignAndBalance(available5, jobState);
        assignmentMap = jobState.toMap();
        assignmentMap.forEach((workerItem, workPoolItems) -> assertTrue(workPoolItems.size() >= 6 && workPoolItems
                .size() <= 7));
        assertEquals(5, assignmentMap.size());
        assertEquals(32, assignmentMap.values().stream().mapToLong(Collection::size).sum());

        jobState = strategy2.reassignAndBalance(available6, jobState);
        assignmentMap = jobState.toMap();
        assignmentMap.forEach((workerItem, workPoolItems) -> assertTrue(workPoolItems.size() >= 5 && workPoolItems
                .size() <= 6));
        assertEquals(6, assignmentMap.size());
        assertEquals(32, assignmentMap.values().stream().mapToLong(Collection::size).sum());

        jobState = strategy2.reassignAndBalance(available7, jobState);
        assignmentMap = jobState.toMap();
        assignmentMap.forEach((workerItem, workPoolItems) -> assertTrue(workPoolItems.size() >= 4 && workPoolItems
                .size() <= 5));
        assertEquals(7, assignmentMap.size());
        assertEquals(32, assignmentMap.values().stream().mapToLong(Collection::size).sum());

        jobState = strategy2.reassignAndBalance(available8, jobState);
        assignmentMap = jobState.toMap();
        assignmentMap.forEach((workerItem, workPoolItems) -> assertEquals(4, workPoolItems.size()));
        assertEquals(8, assignmentMap.size());
        assertEquals(32, assignmentMap.values().stream().mapToLong(Collection::size).sum());

        jobState = strategy2.reassignAndBalance(available1, jobState);
        assignmentMap = jobState.toMap();
        assignmentMap.forEach((workerItem, workPoolItems) -> assertEquals(8, workPoolItems.size()));
        assertEquals(1, assignmentMap.size());
        assertEquals(8, assignmentMap.values().stream().mapToLong(Collection::size).sum());

        jobState = strategy2.reassignAndBalance(available2, jobState);
        assignmentMap = jobState.toMap();
        assignmentMap.forEach((workerItem, workPoolItems) -> assertEquals(8, workPoolItems.size()));
        assertEquals(2, assignmentMap.size());
        assertEquals(16, assignmentMap.values().stream().mapToLong(Collection::size).sum());
    }
}
