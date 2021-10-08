package ru.fix.distributed.job.manager.model;

import ru.fix.distributed.job.manager.JobId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * On which of workers particular job can be executed.
 */
public class Availability extends HashMap<JobId, HashSet<WorkerId>> {
    public static Availability of(Map<JobId, HashSet<WorkerId>> map) {
        Availability availability = new Availability();
        for (Entry<JobId, HashSet<WorkerId>> entry : map.entrySet()) {
            availability.put(entry.getKey(), new HashSet<>(entry.getValue()));
        }
        return availability;
    }
}
