package tech.ydb.yoj.repository.db.list;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.repository.db.Entity;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static lombok.AccessLevel.PROTECTED;

/**
 * Common functionality for ListResult and ViewListResult: implementation of Iterable, last() first().
 *
 * @param <T> list request type
 * @param <R> list result type (might be the same as T)
 */
@Getter
@RequiredArgsConstructor(access = PROTECTED)
public abstract class GenericListResult<T, R> implements Iterable<R> {
    /**
     * Result entries. This list might be empty.
     */
    @NonNull
    private final List<R> entries;

    @NonNull
    private final Schema<R> resultSchema;

    /**
     * Tells whether this result page is the last or not.
     */
    private final boolean lastPage;

    /**
     * Listing request that produced this result.
     */
    @NonNull
    private final ListRequest<T> request;

    @NonNull
    @Override
    public final Iterator<R> iterator() {
        return entries.iterator();
    }

    @Override
    public final void forEach(@NonNull Consumer<? super R> action) {
        entries.forEach(action);
    }

    @NonNull
    @Override
    public final Spliterator<R> spliterator() {
        return entries.spliterator();
    }

    @NonNull
    public final Stream<R> stream() {
        return entries.stream();
    }

    public final int size() {
        return entries.size();
    }

    public final boolean isEmpty() {
        return entries.isEmpty();
    }

    public final R first() {
        if (entries.isEmpty()) {
            throw new NoSuchElementException();
        }
        return entries.get(0);
    }

    public final R last() {
        if (entries.isEmpty()) {
            throw new NoSuchElementException();
        }
        return entries.get(entries.size() - 1);
    }

    @NonNull
    public ListRequest.ListingParams<T> getParams() {
        return request.getParams();
    }

    @NonNull
    public Schema<T> getRequestSchema() {
        return request.getSchema();
    }

    public abstract static class Builder<T extends Entity<T>, R, Target> {
        protected List<R> entries;
        protected boolean lastPage;
        protected final ListRequest<T> request;

        Builder(ListRequest<T> request) {
            this.request = request;
        }

        @NonNull
        public Builder<T, R, Target> entries(@NonNull List<R> entries) {
            this.entries = entries;
            return this;
        }

        @NonNull
        public Builder<T, R, Target> lastPage(boolean lastPage) {
            this.lastPage = lastPage;
            return this;
        }

        @NonNull
        public abstract Target build();

        @NonNull
        public String toString() {
            return "Builder(entries=" + this.entries + ", lastPage=" + this.lastPage + ", request=" + this.request + ")";
        }
    }
}
