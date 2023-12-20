package tech.ydb.yoj.repository.db.list;

import lombok.NonNull;
import tech.ydb.yoj.repository.db.Entity;

import java.util.List;
import java.util.function.UnaryOperator;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

/**
 * Listing result page.
 */
public final class ListResult<T> extends GenericListResult<T, T> implements Iterable<T> {
    private ListResult(@NonNull List<T> entries, boolean lastPage, @NonNull ListRequest<T> request) {
        super(entries, request.getSchema(), lastPage, request);
    }

    @NonNull
    public static <T extends Entity<T>> ListResult<T> empty(@NonNull ListRequest<T> request) {
        return new ListResult<>(emptyList(), true, request);
    }

    @NonNull
    public static <T extends Entity<T>> Builder<T, T, ListResult<T>> builder(@NonNull ListRequest<T> request) {
        return new ListResultBuilder<>(request);
    }

    @NonNull
    public static <T extends Entity<T>> ListResult<T> forPage(@NonNull ListRequest<T> request,
                                                              @NonNull List<T> entries) {
        int pageSize = request.getPageSize();
        boolean lastPage = entries.size() <= pageSize;
        List<T> itemsToReturn = lastPage ? entries : entries.subList(0, pageSize);
        return new ListResult<>(itemsToReturn, lastPage, request);
    }

    @NonNull
    public ListResult<T> returnWithParams(@NonNull ListRequest.ListingParams<T> overrideParams) {
        return new ListResult<>(this.getEntries(), isLastPage(), getRequest().withParams(overrideParams));
    }

    @NonNull
    public ListResult<T> transform(@NonNull UnaryOperator<T> transform) {
        return new ListResult<>(this.stream().map(transform).collect(toList()), isLastPage(), getRequest());
    }

    public static final class ListResultBuilder<T extends Entity<T>> extends Builder<T, T, ListResult<T>> {
        private ListResultBuilder(ListRequest<T> request) {
            super(request);
        }

        @NonNull
        @Override
        public ListResult<T> build() {
            return new ListResult<>(entries, lastPage, request);
        }
    }
}
