package tech.ydb.yoj.repository.db.exception;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

import java.util.Collection;
import java.util.List;

import static java.util.stream.Collectors.joining;

public class AggregateRepositoryException extends RepositoryException {
    @Getter
    private final List<RepositoryException> causes;

    private AggregateRepositoryException(Collection<? extends RepositoryException> causes) {
        super("Encountered " + causes.size() + " repository exception(s): " + causes
                .stream()
                .map(ex -> String.format("\"%s\"", ex))
                .collect(joining(", ")));
        Preconditions.checkArgument(!causes.isEmpty(), "cannot throw aggregate exception without causes");
        this.causes = ImmutableList.copyOf(causes);

        this.causes.forEach(this::addSuppressed);
    }

    public static void throwIfNeeded(Collection<? extends RepositoryException> causes)
            throws RepositoryException {
        if (causes.isEmpty()) {
            return;
        }
        if (causes.size() == 1) {
            throw causes.iterator().next();
        }
        throw new AggregateRepositoryException(causes);
    }
}
