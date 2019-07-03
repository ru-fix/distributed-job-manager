package ru.fix.cpapsm.commons.distributed.job.manager;

import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Kamil Asfandiyarov
 */
public class MinimizedReassignmentAssignmentStrategyTest {

    static void populate(Map<String, Set<String>> map, String worker, String... jobs) {
        map.put(worker, new HashSet<>(Arrays.asList(jobs)));
    }

    final MinimizedReassignmentAssignmentStrategy strategy = new MinimizedReassignmentAssignmentStrategy();


    Map<String, Set<String>> available = new HashMap<>();
    Map<String, Set<String>> current = new HashMap<>();
    Map<String, Set<String>> expected = new HashMap<>();

    @Before
    public void clearMaps() {
        available.clear();
        current.clear();
        expected.clear();
    }

    @Test
    public void lexicographical_case() throws Exception {

        populate(current, "w1", "1", "2");

        populate(available, "w1", "1", "2");
        populate(available, "w2", "1", "2");

        populate(expected, "w1", "1");
        populate(expected, "w2", "2");

        assertEquals(expected, strategy.reassignAndBalance(available, current));
    }

    @Test
    public void sticky_workload_reverse_lexicographical() throws Exception {

        populate(current, "w2", "2");
        populate(current, "w3", "1", "3");

        populate(available, "w2", "1", "2", "3");
        populate(available, "w3", "1", "2", "3");
        populate(available, "w1", "1", "3");

        populate(expected, "w2", "2");
        populate(expected, "w3", "1");
        populate(expected, "w1", "3");

        assertEquals(expected, strategy.reassignAndBalance(available, current));
    }


    @Test
    public void init_balance() throws Exception {


        populate(available, "w1", "1", "2", "3", "4", "5");
        populate(available, "w2", "1", "2", "3", "4", "5");
        populate(available, "w3", "1", "2", "3", "4", "5");

        Map<String, Set<String>> assignment = strategy.reassignAndBalance(available, current);

        assertTrue(assignment.get("w1").size() <= 2);
        assertTrue(assignment.get("w2").size() <= 2);
        assertTrue(assignment.get("w3").size() <= 2);

        assertEquals("count of jobs", 5, assignment.get("w1").size() + assignment.get("w2").size() +
                assignment.get("w3").size());

        assertEquals("sum of jobs", 15, assignment.values().stream()
                .flatMap(Collection::stream)
                .map(Integer::parseInt)
                .reduce((a, b) -> a + b)
                .orElse(0)
                .intValue());

    }


}