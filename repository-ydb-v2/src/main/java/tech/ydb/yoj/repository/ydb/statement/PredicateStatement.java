package tech.ydb.yoj.repository.ydb.statement;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import lombok.Getter;
import lombok.NonNull;
import tech.ydb.proto.ValueProtos;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.databind.schema.Schema.JavaField;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.TableDescriptor;
import tech.ydb.yoj.repository.ydb.yql.YqlCompositeType;
import tech.ydb.yoj.repository.ydb.yql.YqlPredicate;
import tech.ydb.yoj.repository.ydb.yql.YqlPredicateParam;
import tech.ydb.yoj.repository.ydb.yql.YqlType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public abstract class PredicateStatement<PARAMS, ENTITY extends Entity<ENTITY>, RESULT>
        extends YqlStatement<PARAMS, ENTITY, RESULT> {
    private final Function<PARAMS, YqlPredicate> getPredicate;

    private final Map<String, PredParam> predParams;

    public PredicateStatement(
            @NonNull TableDescriptor<ENTITY> tableDescriptor,
            @NonNull EntitySchema<ENTITY> schema,
            @NonNull Schema<RESULT> outSchema,
            @NonNull PARAMS params,
            @NonNull Function<PARAMS, YqlPredicate> getPredicate
    ) {
        super(tableDescriptor, schema, outSchema);

        this.getPredicate = getPredicate;
        YqlPredicate pred = getPredicate.apply(params);

        ImmutableMap.Builder<String, PredParam> bldr = ImmutableMap.builder();
        int index = 0;
        for (YqlPredicateParam<?> p : pred.paramList()) {
            EntitySchema.JavaField rootField = schema.getField(p.getFieldPath());

            int fIndex = index++;

            if (p.getComplexField() == ComplexField.FLATTEN
                    || (p.getComplexField() == ComplexField.TUPLE && rootField.isFlat())) {
                rootField.flatten()
                        .map(jf -> new PredParam(
                                wrapCollectionType(YqlType.of(jf), p.getCollectionKind()),
                                jf.getName(),
                                fIndex,
                                p.isOptional(),
                                false,
                                rootField
                        ))
                        .forEach(pp -> bldr.put(pp.getName(), pp));
            } else if (p.getComplexField() == ComplexField.TUPLE) {
                var param = new PredParam(
                        wrapCollectionType(YqlCompositeType.tuple(rootField), p.getCollectionKind()),
                        rootField.getName(),
                        fIndex,
                        p.isOptional(),
                        true,
                        rootField
                );
                bldr.put(param.getName(), param);
            } else {
                throw new UnsupportedOperationException("Unsupported complex field kind: " + p.getComplexField());
            }
        }
        this.predParams = bldr.build();
    }

    private YqlType wrapCollectionType(YqlType itemType, CollectionKind collectionKind) {
        return switch (collectionKind) {
            case DICT_SET -> YqlCompositeType.dictSet(itemType);
            case LIST -> YqlCompositeType.list(itemType);
            case SINGLE -> itemType;
        };
    }

    @Override
    protected Collection<YqlStatementParam> getParams() {
        return new ArrayList<>(getYqlStatementParams());
    }

    private YqlPredicate getPredicate(PARAMS params) {
        return getPredicate.apply(params);
    }

    @Override
    protected String declarations() {
        return getYqlStatementParams().stream()
                .map(p -> getDeclaration(p.getVar(), p.getYqlTypeName()))
                .collect(joining());
    }

    private Collection<? extends Param> getYqlStatementParams() {
        return predParams.values();
    }

    @Override
    protected ValueProtos.TypedValue createTQueryParameter(YqlType yqlType, Object o, boolean optional) {
        return ValueProtos.TypedValue.newBuilder()
                .setType(getYqlTypeForValue(yqlType, o, optional))
                .setValue(getYqlValue(yqlType, o))
                .build();
    }

    private ValueProtos.Type.Builder getYqlTypeForValue(YqlType type, Object o, boolean optional) {
        ValueProtos.Type.Builder yqlType = getYqlType(type, optional);
        if (optional) {
            return ValueProtos.Type.newBuilder()
                    .setOptionalType(ValueProtos.OptionalType.newBuilder().setItem(yqlType));
        }
        return yqlType;
    }

    @Override
    protected ValueProtos.Type.Builder getYqlType(YqlType yqlType, boolean optional) {
        return yqlType.getYqlTypeBuilder();
    }

    @Override
    protected ValueProtos.Value.Builder getYqlValue(YqlType type, Object value) {
        return type.toYql(value);
    }

    @Override
    public Map<String, ValueProtos.TypedValue> toQueryParameters(PARAMS params) {
        return getYqlStatementParams().stream()
                .collect(toMap(
                        YqlStatementParam::getVar,
                        param -> createTQueryParameter(param.getType(), getParamValue(params, param), param.isOptional())
                ));
    }

    private Object getParamValue(PARAMS params, Param param) {
        PredParam predParam = predParams.get(param.getName());
        if (predParam == null) {
            throw new IllegalStateException("Unknown parameter: " + param);
        }
        return predParam.getValue(getPredicate(params));
    }

    @Getter
    static class Param extends YqlStatementParam {
        private static final Pattern ILLEGAL_SYMBOL_PATTERN = Pattern.compile("[^\\w]");

        Param(YqlType type, String name, boolean optional) {
            super(type, name, optional);
        }

        String getYqlTypeName() {
            return getType().getYqlTypeName();
        }

        /**
         * Returns the value for statement parameter for the specified field or one of its subfields.
         *
         * @param rootField        field to search for the field with specified name
         * @param fieldName        {@link JavaField#getName() name} of the field or one of its subfields
         * @param compositeYqlType true when composite YQL type expected (like Tuple), so value should not be flattened
         * @param paramValue       parameter value
         * @return {@code paramValue}, possibly transformed, if the field is present; {@code null} otherwise
         */
        Object getParamValue(JavaField rootField, String fieldName, boolean compositeYqlType, Object paramValue) {
            if (paramValue instanceof Iterable) {
                return Streams.stream((Iterable<?>) paramValue)
                        .map(v -> getValueForField(rootField, fieldName, compositeYqlType, v))
                        .filter(Objects::nonNull)
                        .collect(toList());
            } else {
                return getValueForField(rootField, fieldName, compositeYqlType, paramValue);
            }
        }

        private Object getValueForField(JavaField rootField, String fieldName, boolean compositeYqlType, Object paramValue) {
            if (!compositeYqlType) {
                if (rootField.getValueType().isComposite() && paramValue.getClass().equals(rootField.getRawType())) {
                    // We got ourselves a wrapper, my friends
                    Map<String, Object> m = new LinkedHashMap<>();
                    rootField.collectValueTo(paramValue, m);
                    return m.get(fieldName);
                } else {
                    return paramValue;
                }
            } else {
                return fieldName.equals(rootField.getName()) ? paramValue : null;
            }
        }

        protected static String underscoreIllegalSymbols(String value) {
            return ILLEGAL_SYMBOL_PATTERN.matcher(value).replaceAll("_");
        }
    }

    private static final class PredParam extends Param {
        private static final String NAME_FORMAT = "pred_%d_%s";

        private final int index;
        private final String fieldName;
        private final boolean compositeYqlType;

        private final EntitySchema.JavaField rootField;

        private PredParam(YqlType fieldType, String fieldName, int index, boolean optional,
                          boolean compositeYqlType, JavaField rootField) {
            super(fieldType, String.format(NAME_FORMAT, index, underscoreIllegalSymbols(fieldName)), optional);
            this.compositeYqlType = compositeYqlType;
            Preconditions.checkArgument(index >= 0, "index must be >= 0");
            this.fieldName = fieldName;
            this.index = index;

            this.rootField = rootField;
        }

        public Object getValue(YqlPredicate predicate) {
            return getParamValue(rootField, fieldName, compositeYqlType, predicate.paramAt(index).getValue());
        }
    }

    public enum ComplexField {
        FLATTEN,
        TUPLE,
    }

    public enum CollectionKind {
        SINGLE,
        DICT_SET,
        LIST,
    }
}
