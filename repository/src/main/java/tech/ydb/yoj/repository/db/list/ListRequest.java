package tech.ydb.yoj.repository.db.list;

import com.google.common.hash.Hashing;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.With;
import tech.ydb.yoj.databind.expression.FilterBuilder;
import tech.ydb.yoj.databind.expression.FilterExpression;
import tech.ydb.yoj.databind.expression.OrderBuilder;
import tech.ydb.yoj.databind.expression.OrderExpression;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.list.token.PageToken;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.function.UnaryOperator;

import static lombok.AccessLevel.PRIVATE;

/**
 * Listing request.
 */
@Value
@RequiredArgsConstructor(access = PRIVATE)
public class ListRequest<T> {
    @With
    long offset;

    @With
    int pageSize;

    Schema<T> schema;

    @With
    ListingParams<T> params;

    @With
    String index;

    public static <T extends Entity<T>> Builder<T> builder(@NonNull Class<T> entityClass) {
        return builder(EntitySchema.of(entityClass));
    }

    public static <T> Builder<T> builder(@NonNull Schema<T> schema) {
        return new Builder<>(schema);
    }

    @Nullable
    public FilterExpression<T> getFilter() {
        return params.filter;
    }

    @Nullable
    public OrderExpression<T> getOrderBy() {
        return params.orderBy;
    }

    @NonNull
    public ListRequest<T> withFilter(@NonNull UnaryOperator<FilterBuilder<T>> filterCtor) {
        return withFilter(filterCtor.apply(FilterBuilder.forSchema(schema)).build());
    }

    public ListRequest<T> withFilter(@Nullable FilterExpression<T> filter) {
        return withParams(params.withFilter(filter));
    }

    @NonNull
    public ListRequest<T> withOrderBy(@NonNull UnaryOperator<OrderBuilder<T>> orderCtor) {
        return withOrderBy(orderCtor.apply(OrderBuilder.forSchema(schema)).build());
    }

    public ListRequest<T> withOrderBy(@Nullable OrderExpression<T> orderBy) {
        return withParams(params.withOrderBy(orderBy));
    }

    public <U extends Entity<U>> ListRequest<U> forEntity(@NonNull Class<U> dstEntityType,
                                                          @NonNull UnaryOperator<String> pathTransformer) {
        Schema<U> dstSchema = EntitySchema.of(dstEntityType);
        return new ListRequest<>(offset, pageSize, dstSchema, params.forSchema(dstSchema, pathTransformer), null);
    }

    @RequiredArgsConstructor(access = PRIVATE)
    public static final class Builder<T> {
        public static final int DEFAULT_PAGE_SIZE = 100;
        public static final int MAX_PAGE_SIZE = 1_000;
        public static final int MAX_SKIP_SIZE = 10_000;

        private final Schema<T> schema;

        private int pageSize = DEFAULT_PAGE_SIZE;
        private long offset = 0L;
        private FilterExpression<T> filter;
        private OrderExpression<T> orderBy;
        private String index;

        private UnaryOperator<Builder<T>> transform;

        @NonNull
        public Builder<T> pageSize(long pageSize) {
            if (pageSize < 1 || pageSize > MAX_PAGE_SIZE) {
                throw new BadListingException.BadPageSize(pageSize, MAX_PAGE_SIZE);
            }

            this.pageSize = Math.toIntExact(pageSize);
            return this;
        }

        @NonNull
        public BuildPageToken pageToken(@NonNull PageToken codec) {
            return new BuildPageToken(codec);
        }

        @NonNull
        public Builder<T> offset(long offset) {
            if (offset < 0 || offset > MAX_SKIP_SIZE) {
                throw new BadListingException.BadOffset(MAX_SKIP_SIZE);
            }
            this.offset = offset;
            return this;
        }

        @NonNull
        public Builder<T> noFilter() {
            return filter((FilterExpression<T>) null);
        }

        @NonNull
        public Builder<T> filter(@NonNull UnaryOperator<FilterBuilder<T>> filterCtor) {
            return filter(filterCtor.apply(FilterBuilder.forSchema(schema)).build());
        }

        @NonNull
        public Builder<T> filter(@Nullable FilterExpression<T> filter) {
            this.filter = filter;
            return this;
        }

        @NonNull
        public Builder<T> index(@Nullable String index) {
            this.index = index;
            return this;
        }

        @NonNull
        public Builder<T> defaultOrder() {
            return orderBy((OrderExpression<T>) null);
        }

        @NonNull
        public Builder<T> orderBy(@NonNull UnaryOperator<OrderBuilder<T>> orderCtor) {
            return orderBy(orderCtor.apply(OrderBuilder.forSchema(schema)).build());
        }

        @NonNull
        public Builder<T> orderBy(@Nullable OrderExpression<T> orderBy) {
            this.orderBy = orderBy;
            return this;
        }

        @NonNull
        public ListRequest<T> build() {
            return (transform == null ? this : transform.apply(this)).build0();
        }

        @NonNull
        private ListRequest<T> build0() {
            return new ListRequest<>(offset, pageSize, schema, params(), index);
        }

        @NonNull
        public ListingParams<T> params() {
            return new ListingParams<>(filter, orderBy);
        }

        @NonNull
        public Schema<T> schema() {
            return schema;
        }

        @RequiredArgsConstructor(access = PRIVATE)
        public final class BuildPageToken {
            private final PageToken codec;

            @NonNull
            public Builder<T> decode(@Nullable String encodedToken) {
                Builder.this.transform = encodedToken == null ? null : bldr -> codec.decode(bldr, encodedToken);
                return Builder.this;
            }
        }
    }

    @Value
    @With
    public static class ListingParams<T> {
        @Nullable
        FilterExpression<T> filter;

        @Nullable
        OrderExpression<T> orderBy;

        @NonNull
        public static <T extends Entity<T>> ListingParams<T> empty() {
            return new ListingParams<>(null, null);
        }

        @NonNull
        public <U extends Entity<U>> ListingParams<U> forSchema(@NonNull Schema<U> dstSchema,
                                                                @NonNull UnaryOperator<String> pathTransformer) {
            return new ListingParams<>(
                    filter == null ? null : filter.forSchema(dstSchema, pathTransformer),
                    orderBy == null ? null : orderBy.forSchema(dstSchema, pathTransformer)
            );
        }

        public long hash() {
            return Hashing.farmHashFingerprint64().newHasher()
                    .putInt(Objects.hashCode(filter))
                    .putInt(Objects.hashCode(orderBy))
                    .hash()
                    .asLong();
        }
    }
}
