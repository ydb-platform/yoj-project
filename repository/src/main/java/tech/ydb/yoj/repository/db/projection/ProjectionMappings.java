package tech.ydb.yoj.repository.db.projection;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntityIdSchema;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.list.ListRequest;
import tech.ydb.yoj.repository.db.list.ListResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static java.util.stream.Collectors.toList;
import static lombok.AccessLevel.PRIVATE;

public final class ProjectionMappings {
    private ProjectionMappings() {
    }

    /**
     * Creates a <em>lenient</em> one-to-one mapping from entity fields to projection fields, which contains all
     * {@link #strictFieldMapping(Class, Class) strict mappings}, plus mappings of the form
     * {@code <entity field> <-> <projection field with the same path>} (if both fields exist).
     *
     * @param projectionType projection class
     * @param entityType     entity class
     * @param <P>            projection type
     * @param <T>            entity type
     * @return Map: Entity field path -> Projection field path
     * @see #strictFieldMapping(Class, Class)
     */
    @NonNull
    public static <P extends Entity<P>, T extends Entity<T>> Map<String, String> lenientFieldMapping(
            @NonNull Class<P> projectionType, @NonNull Class<T> entityType) {
        EntitySchema<T> entitySchema = EntitySchema.of(entityType);
        Class<?> entityIdType = entitySchema.getIdSchema().getType();

        BiMap<String, String> mapping = HashBiMap.create(strictFieldMapping(projectionType, entityType));
        EntitySchema.of(projectionType).flattenFields()
                .stream()
                .filter(pf -> !isEntityId(pf, entityIdType) && !mapping.inverse().containsKey(pf.getPath()))
                .forEach(pf -> findMatchingNonIdField(pf, entitySchema)
                        .ifPresent(ef -> mapping.put(ef.getPath(), pf.getPath()))
                );
        return mapping;
    }

    /**
     * Creates a one-to-one mapping from entity fields to entity projection ID fields, assuming that the projection ID
     * contains fields with the same name as in the main entity and <em>at most one</em> field for the main entity ID
     * (with any name).
     * </ul>
     * <p>
     * <em>E.g.</em>, the following entity-projection pair qualifies: <blockquote><pre>
     * &#64;Value class MyEntity implements Entity&lt;MyEntity> {
     *     Id id;
     *     String field;
     *
     *     &#64;Value static class Id implements Entity.Id&lt;MyEntity> { String value; }
     * }
     *
     * &#64;Value class MyIndex implements Entity&lt;MyIndex> {
     *     Id id;
     *
     *     &#64;Value
     *     static class Id implements Entity.Id&lt;MyIndex> {
     *         // MUST have the same Java field name as in the entity class
     *         // (DB name specified in &#64;Column annotation does not matter.)
     *         String field;
     *
     *         // OPTIONAL. If present, this field MAY have any name
     *         MyEntity.Id entityId;
     *     }
     * }
     * </pre></blockquote>
     *
     * @param projectionType projection class
     * @param entityType     entity class
     * @param <P>            projection type
     * @param <T>            entity type
     * @return Bidirectional mapping: Entity field path -> Projection field path
     */
    @NonNull
    public static <P extends Entity<P>, T extends Entity<T>> Map<String, String> strictFieldMapping(
            @NonNull Class<P> projectionType, @NonNull Class<T> entityType) {
        EntitySchema<T> entitySchema = EntitySchema.of(entityType);
        Class<?> entityIdType = entitySchema.getIdSchema().getType();
        List<Schema.JavaField> projectionIdFields = EntityIdSchema.ofEntity(projectionType).getFields();

        Map<String, String> mapping = new HashMap<>();

        projectionIdFields
                .stream()
                .filter(f -> !isEntityId(f, entityIdType))
                .flatMap(Schema.JavaField::flatten)
                .forEach(f -> mapping.put(getMatchingNonIdField(f, entitySchema).getPath(), f.getPath()));

        List<Schema.JavaField> idTypeFields = projectionIdFields.stream()
                .filter(f -> isEntityId(f, entityIdType))
                .collect(toList());
        if (idTypeFields.size() > 1) {
            throw new IllegalStateException("Projection ID cannot have more than 1 field with type: " + entityIdType);
        }
        if (idTypeFields.size() == 0) {
            return mapping;
        }
        idTypeFields.get(0).flatten().forEach(idPart ->
                mapping.put(getMatchingIdField(idPart, entitySchema).getPath(), idPart.getPath()));

        return mapping;
    }

    private static boolean isEntityId(@NonNull Schema.JavaField field, @NonNull Class<?> entityIdType) {
        return field.getType().equals(entityIdType);
    }

    @NonNull
    private static Optional<Schema.JavaField> findMatchingNonIdField(@NonNull Schema.JavaField projectionField,
                                                                     @NonNull EntitySchema<?> realEntity) {
        // Entity.<field> <-> Projection.id.<field>
        return realEntity.findField(projectionField.getRawPath());
    }

    @NonNull
    private static Schema.JavaField getMatchingNonIdField(@NonNull Schema.JavaField projectionField,
                                                          @NonNull EntitySchema<?> realEntity) {
        // Entity.<field> <-> Projection.id.<field>
        return realEntity.getField(projectionField.getRawPath());
    }

    @NonNull
    private static Schema.JavaField getMatchingIdField(@NonNull Schema.JavaField projectionIdField,
                                                       @NonNull EntitySchema<?> realEntity) {
        // Entity.id[.<subfield>] <-> Projection.id.<entity ID-typed field name>[.<subfield>]
        return realEntity.getIdSchema().getField(projectionIdField.getRawSubPath(1));
    }

    @NonNull
    public static <P extends Entity<P>> ListViaProjection<P> listViaProjection(@NonNull Class<P> projectionType) {
        return new ListViaProjection<>(projectionType);
    }

    @RequiredArgsConstructor(access = PRIVATE)
    public static final class ListViaProjection<P extends Entity<P>> {
        private final Class<P> projectionType;

        @NonNull
        public <T extends Entity<T>> Listing<T, P> entities(@NonNull ListRequest<T> request) {
            return entities(request, lenientFieldMapping(projectionType, request.getSchema().getType()));
        }

        @NonNull
        public <T extends Entity<T>> Listing<T, P> entities(@NonNull ListRequest<T> request,
                                                            @NonNull Map<String, String> fieldMapping) {
            return entities(request, f -> getFieldMapping(fieldMapping, f));
        }

        @NonNull
        private String getFieldMapping(@NonNull Map<String, String> fieldMapping, String f) {
            String mapping = fieldMapping.get(f);
            Preconditions.checkState(mapping != null,
                    "No mapping for entity field \"%s\" in projection %s", f, projectionType);
            return mapping;
        }

        @NonNull
        public <T extends Entity<T>> Listing<T, P> entities(@NonNull ListRequest<T> request,
                                                            @NonNull UnaryOperator<String> fieldMapping) {
            return new Listing<>(request, request.forEntity(projectionType, fieldMapping));
        }

        @RequiredArgsConstructor(access = PRIVATE)
        public static final class Listing<T extends Entity<T>, P extends Entity<P>> {
            private final ListRequest<T> request;
            private final ListRequest<P> projRequest;

            @NonNull
            public TransformedListing<T, P> transforming(@NonNull Function<P, T> unproject) {
                return new TransformedListing<>(this, unproject);
            }

            @NonNull
            public ListResult<P> run(@NonNull Function<ListRequest<P>, ListResult<P>> listFunc) {
                return listFunc.apply(projRequest);
            }
        }

        @RequiredArgsConstructor(access = PRIVATE)
        public static final class TransformedListing<T extends Entity<T>, P extends Entity<P>> {
            private final Listing<T, P> listing;
            private final Function<P, T> unproject;

            @NonNull
            public ListResult<T> run(@NonNull Function<ListRequest<P>, ListResult<P>> listFunc) {
                ListResult<P> projResult = listing.run(listFunc);
                return ListResult.builder(listing.request)
                        .entries(projResult.stream().map(unproject).collect(toList()))
                        .lastPage(projResult.isLastPage())
                        .build();
            }
        }
    }
}
