package ru.fix.distributed.job.manager.model;

import org.jetbrains.annotations.NotNull;
import ru.fix.distributed.job.manager.IdentityValidator;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class WorkerId implements Comparable<WorkerId> {

    private final String id;

    public WorkerId(String id) {
        IdentityValidator.validate(IdentityValidator.IdentityType.WorkerId, id);
        this.id = id;
    }

    public static Set<WorkerId> setOf(String... ids) {
        return Arrays.stream(ids).map(WorkerId::new).collect(Collectors.toSet());
    }

    public String getId() {
        return id;
    }

    @Override
    public int compareTo(@NotNull WorkerId o) {
        Objects.requireNonNull(o);
        return this.id.compareTo(o.getId());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WorkerId that = (WorkerId) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "Worker[" + id + "]";
    }
}
