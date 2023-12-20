package tech.ydb.yoj.repository.db.list;

import lombok.NonNull;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.Table;
import tech.ydb.yoj.repository.db.ViewSchema;

import java.util.List;

/**
 * Listing result page for entity view.
 */
public final class ViewListResult<T extends Entity<T>, V extends Table.View> extends GenericListResult<T, V> {
    private ViewListResult(@NonNull List<V> entries, @NonNull Schema<V> viewSchema, boolean lastPage, @NonNull ListRequest<T> request) {
        super(entries, viewSchema, lastPage, request);
    }

    @NonNull
    public static <T extends Entity<T>, V extends Table.View> Builder<T, V, ViewListResult<T, V>> builder(
            @NonNull Class<V> viewType,
            @NonNull ListRequest<T> request
    ) {
        return new ViewListResultBuilder<>(viewType, request);
    }

    @NonNull
    public static <T extends Entity<T>, V extends Table.View> ViewListResult<T, V> forPage(
            @NonNull ListRequest<T> request,
            @NonNull Class<V> viewClass,
            @NonNull List<V> entries
    ) {
        int pageSize = request.getPageSize();
        boolean lastPage = entries.size() <= pageSize;
        List<V> itemsToReturn = lastPage ? entries : entries.subList(0, pageSize);
        return new ViewListResult<>(itemsToReturn, ViewSchema.of(viewClass), lastPage, request);
    }

    @NonNull
    public ViewListResult<T, V> returnWithParams(@NonNull ListRequest.ListingParams<T> overrideParams) {
        return new ViewListResult<>(this.getEntries(), this.getResultSchema(), isLastPage(), getRequest().withParams(overrideParams));
    }

    public static final class ViewListResultBuilder<T extends Entity<T>, V extends Table.View> extends Builder<T, V, ViewListResult<T, V>> {
        private final Class<V> viewType;

        private ViewListResultBuilder(Class<V> viewType, ListRequest<T> request) {
            super(request);

            this.viewType = viewType;
        }

        @NonNull
        @Override
        public ViewListResult<T, V> build() {
            return new ViewListResult<>(entries, ViewSchema.of(viewType), lastPage, request);
        }
    }
}
