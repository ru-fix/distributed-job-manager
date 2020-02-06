package ru.fix.distributed.job.manager;

import ru.fix.distributed.job.manager.annotation.JobIdField;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

class JobIdsHolder {
    private JobIdsHolder(){}

    private static Map<DistributedJob, String> jobIds;

    static String getId(DistributedJob job){
        if(jobIds == null) throw new IllegalStateException("Job ids are not loaded");
        return jobIds.get(job);
    }

    static void loadIds(Collection<DistributedJob> distributedJobs){
        jobIds = distributedJobs
                .stream()
                .collect(Collectors.toMap(job -> job, JobIdsHolder::getIdInternal));
    }

    private static String getIdInternal(DistributedJob job) {
        for(Field field : job.getClass().getDeclaredFields()) {
            if(field.isAnnotationPresent(JobIdField.class)){
                try {
                    field.setAccessible(true);
                    return (String) field.get(job);
                } catch (Exception e) {
                    throw new IllegalStateException("Some troubles with getting jobId by annotation. Maybe it's not a String field?", e);
                }
            }
        }
        return job.getClass().getName();
    }
}
