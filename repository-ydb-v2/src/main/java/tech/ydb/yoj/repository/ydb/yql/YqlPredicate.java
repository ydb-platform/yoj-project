package tech.ydb.yoj.repository.ydb.yql;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import tech.ydb.yoj.DeprecationWarnings;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.ydb.statement.PredicateStatement;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static lombok.AccessLevel.PRIVATE;
import static tech.ydb.yoj.repository.ydb.yql.YqlPredicate.IsNullPredicate.IsNullType.IS_NOT_NULL;
import static tech.ydb.yoj.repository.ydb.yql.YqlPredicate.IsNullPredicate.IsNullType.IS_NULL;
import static tech.ydb.yoj.repository.ydb.yql.YqlPredicate.LikePredicate.Type.LIKE;
import static tech.ydb.yoj.repository.ydb.yql.YqlPredicate.LikePredicate.Type.NOT_LIKE;
import static tech.ydb.yoj.repository.ydb.yql.YqlPredicate.Rel.EQ;
import static tech.ydb.yoj.repository.ydb.yql.YqlPredicate.Rel.GT;
import static tech.ydb.yoj.repository.ydb.yql.YqlPredicate.Rel.GTE;
import static tech.ydb.yoj.repository.ydb.yql.YqlPredicate.Rel.LT;
import static tech.ydb.yoj.repository.ydb.yql.YqlPredicate.Rel.LTE;
import static tech.ydb.yoj.repository.ydb.yql.YqlPredicate.Rel.NEQ;

/**
 * Represents a <em>predicate</em>: a boolean expression that can appear in the {@code WHERE} YQL clause.
 * <p>
 * To return valid YQL template for the predicate, including references to statement parameters (as {@code ?})
 * and entity fields (as {@code {field.subfield}}), call {@link #toYql(EntitySchema) toYql()}.
 * <p>
 * To construct simple predicates, use static methods of this class, e.g.,
 * {@link #in(String, Collection) in()} and {@link #eq(String, Object)}.<br>
 * <br>
 * To invert your predicate, call {@link YqlPredicate#negate() negate()} (instance method) or
 * {@link YqlPredicate#not(YqlPredicate) not(pred)} (static method).
 * <br>
 * To combine predicates, use the {@link YqlPredicate#and(YqlPredicate, YqlPredicate...) and()} and
 * {@link YqlPredicate#or(YqlPredicate, YqlPredicate...) or()} (both static and instance methods are available).
 */
public abstract class YqlPredicate implements YqlStatementPart<YqlPredicate> {
    public static final String TYPE = "Predicate";
    private static final AtomicBoolean useLegacyIn = new AtomicBoolean(false);
    private static final AtomicBoolean useLegacyRel = new AtomicBoolean(false);

    /**
     * @deprecated This method will be removed in YOJ 3.0.0. There is no alternative, just stop calling it.
     */
    @Deprecated(forRemoval = true)
    public static void setUseLegacyIn(boolean value) {
        DeprecationWarnings.warnOnce("YqlPredicate.setUseLegacyIn(boolean)",
                "You are using YqlPredicate.setUseLegacyIn(boolean) which is deprecated for removal in YOJ 3.0.0. Please stop calling this method");
        YqlPredicate.useLegacyIn.set(value);
    }

    /**
     * @deprecated This method will be removed in YOJ 3.0.0. There is no alternative, just stop calling it.
     */
    @Deprecated(forRemoval = true)
    public static void setUseLegacyRel(boolean value) {
        DeprecationWarnings.warnOnce("YqlPredicate.setUseLegacyRel(boolean)",
                "You are using YqlPredicate.setUseLegacyRel(boolean) which is deprecated for removal in YOJ 3.0.0. Please stop calling this method");
        YqlPredicate.useLegacyRel.set(value);
    }

    public static YqlPredicate from(Collection<? extends YqlStatementPart<?>> parts) {
        return parts.stream()
                .filter(p -> p instanceof YqlPredicate)
                .map(YqlPredicate.class::cast)
                .reduce(YqlPredicate.alwaysTrue(), (p1, p2) -> p1.and(p2));
    }

    public static FieldPredicateBuilder where(@NonNull String fieldPath) {
        return new FieldPredicateBuilder(fieldPath, UnaryOperator.identity());
    }

    public static YqlPredicate not(@NonNull YqlPredicate pred) {
        return pred.negate();
    }

    public static YqlPredicate and(@NonNull YqlPredicate first, @NonNull YqlPredicate... rest) {
        return and(ImmutableList.<YqlPredicate>builder().add(first).add(rest).build());
    }

    public static YqlPredicate and(@NonNull Collection<YqlPredicate> predicates) {
        return predicates.isEmpty() ? alwaysTrue() : new AndPredicate(predicates);
    }

    public static YqlPredicate or(@NonNull YqlPredicate first, @NonNull YqlPredicate... rest) {
        return or(ImmutableList.<YqlPredicate>builder().add(first).add(rest).build());
    }

    public static YqlPredicate or(@NonNull Collection<YqlPredicate> predicates) {
        return new OrPredicate(predicates);
    }

    private static YqlPredicate isNull(String fieldPath) {
        return new IsNullPredicate(fieldPath, IS_NULL);
    }

    private static YqlPredicate isNotNull(String fieldPath) {
        return new IsNullPredicate(fieldPath, IS_NOT_NULL);
    }

    @SafeVarargs
    public static <T> YqlPredicate in(@NonNull String fieldPath,
                                      @NonNull T possibleValue, @NonNull T... restOfPossibleValues) {
        return in(fieldPath, ImmutableList.<T>builder().add(possibleValue).add(restOfPossibleValues).build());
    }

    public static <T> YqlPredicate in(@NonNull String fieldPath, @NonNull Collection<@NonNull ? extends T> values) {
        return values.isEmpty() ? alwaysFalse() : inPredicate(fieldPath, ImmutableList.copyOf(values), InType.IN);
    }

    @SafeVarargs
    private static <T> YqlPredicate notIn(@NonNull String fieldPath,
                                          @NonNull T possibleValue, @NonNull T... restOfPossibleValues) {
        return notIn(fieldPath, ImmutableList.<T>builder().add(possibleValue).add(restOfPossibleValues).build());
    }

    private static <T> YqlPredicate notIn(@NonNull String fieldPath, @NonNull Collection<@NonNull ? extends T> values) {
        return values.isEmpty() ? alwaysTrue() : inPredicate(fieldPath, ImmutableList.copyOf(values), InType.NOT_IN);
    }

    public static <T> YqlPredicate eq(@NonNull String fieldPath, @Nullable T value) {
        return value == null ? isNull(fieldPath) : relPredicate(EQ, fieldPath, value);
    }

    private static <T> YqlPredicate inPredicate(@NonNull String fieldPath, @NonNull Collection<T> values, @NonNull YqlPredicate.InType inType) {
        return useLegacyIn.get()
                ? new InLegacyPredicate<>(fieldPath, values, inType)
                : new InPredicate<>(fieldPath, values, inType);
    }

    public static <T> YqlPredicate neq(@NonNull String fieldPath, @Nullable T value) {
        return value == null ? isNotNull(fieldPath) : relPredicate(NEQ, fieldPath, value);
    }

    public static <T> YqlPredicate lt(@NonNull String fieldPath, @NonNull T value) {
        return relPredicate(LT, fieldPath, value);
    }

    public static <T> YqlPredicate lte(@NonNull String fieldPath, @NonNull T value) {
        return relPredicate(LTE, fieldPath, value);
    }

    public static <T> YqlPredicate gt(@NonNull String fieldPath, @NonNull T value) {
        return relPredicate(GT, fieldPath, value);
    }

    public static <T> YqlPredicate gte(@NonNull String fieldPath, @NonNull T value) {
        return relPredicate(GTE, fieldPath, value);
    }

    @NonNull
    private static <T> YqlPredicate relPredicate(Rel rel, @NonNull String fieldPath, @NonNull T value) {
        return useLegacyRel.get()
                ? new LegacyRelPredicate<>(rel, fieldPath, value)
                : new RelPredicate<>(rel, fieldPath, value);
    }

    public static YqlPredicate like(@NonNull String fieldPath, @NonNull String value) {
        return like(fieldPath, value, null);
    }

    public static YqlPredicate like(@NonNull String fieldPath, @NonNull String value, @Nullable Character escape) {
        return new LikePredicate<>(LIKE, fieldPath, value, escape);
    }

    public static YqlPredicate notLike(@NonNull String fieldPath, @NonNull String value) {
        return notLike(fieldPath, value, null);
    }

    public static YqlPredicate notLike(@NonNull String fieldPath, @NonNull String value, @Nullable Character escape) {
        return new LikePredicate<>(NOT_LIKE, fieldPath, value, escape);
    }

    public static YqlPredicate alwaysTrue() {
        return TruePredicate.INSTANCE;
    }

    public static YqlPredicate alwaysFalse() {
        return FalsePredicate.INSTANCE;
    }

    /**
     * @param schema entity schema
     * @return non-null, valid YQL for the predicate, possibly including reference to some positional statement
     * parameters as {@code ?} and entity field references as {@code {field.path}}
     */
    @Override
    public abstract <T extends Entity<T>> String toYql(@NonNull EntitySchema<T> schema);

    @Override
    public final String getType() {
        return TYPE;
    }

    @Override
    public final String getYqlPrefix() {
        return "WHERE ";
    }

    @Override
    public final int getPriority() {
        return 50;
    }

    @Override
    public final List<? extends YqlStatementPart<?>> combine(@NonNull List<? extends YqlPredicate> others) {
        List<YqlPredicate> andPredicates = ImmutableList.<YqlPredicate>builder()
                .add(this)
                .addAll(others)
                .build();
        return ImmutableList.of(and(andPredicates));
    }

    /**
     * @return stream of statement parameter specifications, if this YQL predicate uses parameters
     */
    public Stream<YqlPredicateParam<?>> paramStream() {
        return Stream.empty();
    }

    public YqlPredicateParam<?> paramAt(int index) {
        return paramList().get(index);
    }

    /**
     * @return list of statement parameter specification, in the same order as {@link #paramStream}
     */
    public List<YqlPredicateParam<?>> paramList() {
        final Spliterator<?> sp = paramStream().spliterator();
        if (sp.getExactSizeIfKnown() == 0) {
            return emptyList();
        } else {
            return paramStream().collect(toList());
        }
    }

    public YqlPredicate negate() {
        return new NotPredicate(this);
    }

    public YqlPredicate and(@NonNull YqlPredicate other) {
        return new AndPredicate(ImmutableList.of(this, other));
    }

    public YqlPredicate or(@NonNull YqlPredicate other) {
        return new OrPredicate(ImmutableList.of(this, other));
    }

    public FieldPredicateBuilder and(@NonNull String fieldPath) {
        return new FieldPredicateBuilder(fieldPath, this::and);
    }

    public FieldPredicateBuilder or(@NonNull String fieldPath) {
        return new FieldPredicateBuilder(fieldPath, this::or);
    }

    /*package*/ enum InType {
        IN("DictContains", "IN"),
        NOT_IN("NOT DictContains", "NOT IN");

        private final String legacyYql;
        private final String yql;

        InType(String legacyYql, String yql) {
            this.legacyYql = legacyYql;
            this.yql = yql;
        }
    }

    private static final class TruePredicate extends YqlPredicate {
        private static final YqlPredicate INSTANCE = new TruePredicate();

        private TruePredicate() {
        }

        @Override
        public <T extends Entity<T>> String toYql(@NonNull EntitySchema<T> schema) {
            return "1 = 1";
        }

        @Override
        public YqlPredicate negate() {
            return alwaysFalse();
        }

        @Override
        public String toString() {
            return "alwaysTrue()";
        }
    }

    private static final class FalsePredicate extends YqlPredicate {
        private static final YqlPredicate INSTANCE = new FalsePredicate();

        private FalsePredicate() {
        }

        @Override
        public <T extends Entity<T>> String toYql(@NonNull EntitySchema<T> schema) {
            return "0 = 1";
        }

        @Override
        public YqlPredicate negate() {
            return alwaysTrue();
        }

        @Override
        public String toString() {
            return "alwaysFalse()";
        }
    }

    @Deprecated(forRemoval = true)
    /*package*/ static final class LegacyRelPredicate<V> extends YqlPredicate {
        private final Rel rel;
        private final String fieldPath;
        private final YqlPredicateParam<V> param;

        private LegacyRelPredicate(@NonNull Rel rel, @NonNull String fieldPath, @NonNull V value) {
            this(
                    rel,
                    fieldPath,
                    YqlPredicateParam.of(fieldPath, value)
            );
        }

        private LegacyRelPredicate(Rel rel, String fieldPath, YqlPredicateParam<V> param) {
            this.rel = rel;
            this.fieldPath = fieldPath;
            this.param = param;
        }

        @Override
        public Stream<YqlPredicateParam<?>> paramStream() {
            return Stream.of(param);
        }

        @Override
        public List<YqlPredicateParam<?>> paramList() {
            return singletonList(param);
        }

        @Override
        public YqlPredicate negate() {
            return new LegacyRelPredicate<>(this.rel.negate(), this.fieldPath, this.param);
        }

        @Override
        public <T extends Entity<T>> String toYql(@NonNull EntitySchema<T> schema) {
            EntitySchema.JavaField field = schema.getField(fieldPath);
            return field.flatten()
                    .map(this::fieldToYql)
                    .reduce(rel::combine)
                    .orElseThrow(() -> new IllegalStateException("No DB fields found for " + fieldPath + " in " + schema.getName()));
        }

        private String fieldToYql(EntitySchema.JavaField field) {
            return format("`%s` %s ?", field.getName(), rel.yql);
        }

        @Override
        public String toString() {
            return format("%s %s %s", fieldPath, rel, param.getValue());
        }
    }

    /*package*/ static final class RelPredicate<V> extends YqlPredicate {
        private final Rel rel;
        private final String fieldPath;
        private final YqlPredicateParam<V> param;

        private RelPredicate(@NonNull Rel rel, @NonNull String fieldPath, @NonNull V value) {
            this(
                    rel,
                    fieldPath,
                    YqlPredicateParam.of(
                            fieldPath,
                            value,
                            false,
                            PredicateStatement.ComplexField.TUPLE,
                            PredicateStatement.CollectionKind.SINGLE
                    )
            );
        }

        private RelPredicate(Rel rel, String fieldPath, YqlPredicateParam<V> param) {
            this.rel = rel;
            this.fieldPath = fieldPath;
            this.param = param;
        }

        @Override
        public Stream<YqlPredicateParam<?>> paramStream() {
            return Stream.of(param);
        }

        @Override
        public List<YqlPredicateParam<?>> paramList() {
            return singletonList(param);
        }

        @Override
        public YqlPredicate negate() {
            return new LegacyRelPredicate<>(this.rel.negate(), this.fieldPath, this.param);
        }

        @Override
        public <T extends Entity<T>> String toYql(@NonNull EntitySchema<T> schema) {
            EntitySchema.JavaField field = schema.getField(fieldPath);
            if (field.isFlat()) {
                return format("`%s` %s ?", field.toFlatField().getName(), rel.yql);
            } else {
                return format("(%s) %s ?", field.flatten()
                                .map(f -> "`" + f.getName() + "`").collect(joining(", ")),
                        rel.yql
                );
            }
        }

        @Override
        public String toString() {
            return format("%s %s %s", fieldPath, rel, param.getValue());
        }
    }

    /*package*/ enum Rel {
        EQ("=", "NEQ", (e1, e2) -> e1 + " AND " + e2),
        NEQ("<>", "EQ", (e1, e2) -> e1 + " OR " + e2),
        LT("<", "GTE"),
        GT(">", "LTE"),
        LTE("<=", "GT"),
        GTE(">=", "LT");

        private final String yql;
        private final String negation;
        private final BiFunction<String, String, String> exprCombiner;

        Rel(String yql, String negation) {
            this(yql, negation, (_1, _2) -> {
                throw new UnsupportedOperationException(yql + " relation is not supported for complex fields");
            });
        }

        Rel(String yql, String negation, BiFunction<String, String, String> exprCombiner) {
            this.yql = yql;
            this.negation = negation;
            this.exprCombiner = exprCombiner;
        }

        public final Rel negate() {
            return Rel.valueOf(negation);
        }

        /**
         * @deprecated Will be removed in YOJ 3.0.0 because combining relational predicates is no longer needed.
         */
        @Deprecated(forRemoval = true)
        public final String combine(String result, String element) {
            return result == null ? element : exprCombiner.apply(result, element);
        }
    }

    @RequiredArgsConstructor
    /*package*/ static final class LikePredicate<V> extends YqlPredicate {
        private final Type type;
        private final String fieldPath;
        private final YqlPredicateParam<V> param;
        private final Character escape; // null if no escape

        private LikePredicate(Type type, @NonNull String fieldPath, @NonNull V value,
                              @Nullable Character escape) {
            this(
                    type,
                    fieldPath,
                    YqlPredicateParam.of(fieldPath, value),
                    validateEscape(escape)
            );
        }

        /**
         * YDB <em>currently</em> (as of 2023-05) does not support {@code \ % _} characters as escape characters
         * in {@code LIKE} operator. You also cannot use {@code ?} because it interferes with {@code YqlStatement}
         * parameter substitution.
         *
         * @param escape escape character to validate; {@code null} means no escape and is always valid
         * @return {@code escape}
         * @throws IllegalArgumentException escape character is invalid
         */
        private static Character validateEscape(@Nullable Character escape) {
            Preconditions.checkArgument(
                    escape == null
                            || (escape != '\\' && escape != '%' && escape != '_' && escape != '?'),
                    "Escape symbol not supported: '%s'", escape
            );
            return escape;
        }

        @Override
        public Stream<YqlPredicateParam<?>> paramStream() {
            return Stream.of(param);
        }

        @Override
        public List<YqlPredicateParam<?>> paramList() {
            return List.of(param);
        }

        @Override
        public YqlPredicate negate() {
            return new LikePredicate<>(this.type.negate(), this.fieldPath, this.param, this.escape);
        }

        @Override
        public <T extends Entity<T>> String toYql(@NonNull EntitySchema<T> schema) {
            EntitySchema.JavaField field = schema.getField(fieldPath).toFlatField();
            StringBuilder sb = new StringBuilder()
                    .append('`').append(field.getName()).append('`')
                    .append(' ').append(type.toYql()).append(' ')
                    .append('?');
            if (escape != null) {
                char strBoundaryChar = escape == '\'' ? '"' : '\'';
                sb.append(" ESCAPE ").append(strBoundaryChar).append(escape).append(strBoundaryChar);
            }
            return sb.toString();
        }

        @Override
        public String toString() {
            return format("%s %s %s%s", fieldPath, type, param.getValue(), escape == null ? "" : " ESCAPE " + escape);
        }

        public enum Type {
            LIKE {
                @Override
                public String toYql() {
                    return "LIKE";
                }

                @Override
                public Type negate() {
                    return NOT_LIKE;
                }
            },
            NOT_LIKE {
                @Override
                public String toYql() {
                    return "NOT LIKE";
                }

                @Override
                public Type negate() {
                    return LIKE;
                }
            };

            public abstract String toYql();

            public abstract Type negate();
        }
    }

    @AllArgsConstructor(access = PRIVATE)
    @Deprecated(forRemoval = true)
    /*package*/ static final class InLegacyPredicate<V> extends YqlPredicate {
        private final YqlPredicateParam<Collection<V>> param;
        private final String fieldPath;
        private final InType inType;

        private InLegacyPredicate(@NonNull String fieldPath, @NonNull Collection<V> values, @NonNull InType inType) {
            this(YqlPredicateParam.of(fieldPath, values), fieldPath, inType);
        }

        @Override
        public Stream<YqlPredicateParam<?>> paramStream() {
            return isEmpty() ? Stream.empty() : Stream.of(param);
        }

        @Override
        public List<YqlPredicateParam<?>> paramList() {
            return isEmpty() ? emptyList() : singletonList(param);
        }

        @Override
        public <T extends Entity<T>> String toYql(@NonNull EntitySchema<T> schema) {
            if (isEmpty()) {
                return alwaysFalse().toString();
            }

            EntitySchema.JavaField field = schema.getField(fieldPath);
            Preconditions.checkArgument(field.isFlat(), "Only flat fields are supported for IN/NOT IN queries");
            return format("%s(?, `%s`)", inType.legacyYql, field.getName());
        }

        @Override
        public YqlPredicate negate() {
            switch (inType) {
                case IN:
                    return new InLegacyPredicate<>(param, fieldPath, InType.NOT_IN);
                case NOT_IN:
                    return new InLegacyPredicate<>(param, fieldPath, InType.IN);
                default:
                    throw new UnsupportedOperationException("This should never happen");
            }
        }

        private boolean isEmpty() {
            return param.getValue().isEmpty();
        }

        @Override
        public String toString() {
            return format("%s %s (%s)", fieldPath, inType, param.getValue());
        }

    }

    @AllArgsConstructor(access = PRIVATE)
    /*package*/ static final class InPredicate<V> extends YqlPredicate {
        private final YqlPredicateParam<Collection<V>> param;
        private final String fieldPath;
        private final InType inType;

        private InPredicate(@NonNull String fieldPath, @NonNull Collection<V> values, @NonNull InType inType) {
            this(YqlPredicateParam.of(
                    fieldPath,
                    values,
                    false,
                    PredicateStatement.ComplexField.TUPLE,
                    PredicateStatement.CollectionKind.LIST
            ), fieldPath, inType);
        }

        @Override
        public Stream<YqlPredicateParam<?>> paramStream() {
            return isEmpty() ? Stream.empty() : Stream.of(param);
        }

        @Override
        public List<YqlPredicateParam<?>> paramList() {
            return isEmpty() ? emptyList() : singletonList(param);
        }

        @Override
        public <T extends Entity<T>> String toYql(@NonNull EntitySchema<T> schema) {
            if (isEmpty()) {
                return alwaysFalse().toString();
            }

            EntitySchema.JavaField field = schema.getField(fieldPath);
            if (field.isFlat()) {
                return format("`%s` %s ?", field.toFlatField().getName(), inType.yql);
            } else {
                return format("(%s) %s ?", field.flatten()
                                .map(f -> "`" + f.getName() + "`").collect(joining(", ")),
                        inType.yql
                );
            }
        }

        @Override
        public YqlPredicate negate() {
            switch (inType) {
                case IN:
                    return new InPredicate<>(param, fieldPath, InType.NOT_IN);
                case NOT_IN:
                    return new InPredicate<>(param, fieldPath, InType.IN);
                default:
                    throw new UnsupportedOperationException("This should never happen");
            }
        }

        private boolean isEmpty() {
            return param.getValue().isEmpty();
        }

        @Override
        public String toString() {
            return format("%s %s (%s)", fieldPath, inType, param.getValue());
        }
    }

    /*package*/ static final class IsNullPredicate extends YqlPredicate {
        private final String fieldPath;
        private final IsNullType type;

        private IsNullPredicate(@NonNull String fieldPath, @NonNull IsNullType type) {
            this.fieldPath = fieldPath;
            this.type = type;
        }

        @Override
        public <T extends Entity<T>> String toYql(@NonNull EntitySchema<T> schema) {
            return schema.getField(fieldPath).flatten()
                    .map(dbField -> format("`%s` %s", dbField.getName(), type.yql))
                    .reduce(type::combine)
                    .orElseThrow(() -> new IllegalStateException("No DB fields found for " + fieldPath + " in " + schema.getName()));
        }

        @Override
        public YqlPredicate negate() {
            switch (type) {
                case IS_NULL:
                    return new IsNullPredicate(fieldPath, IS_NOT_NULL);
                case IS_NOT_NULL:
                    return new IsNullPredicate(fieldPath, IS_NULL);
                default:
                    throw new UnsupportedOperationException("This should never happen");
            }
        }

        @Override
        public String toString() {
            return format("%s %s", fieldPath, type);
        }

        /*package*/ enum IsNullType {
            IS_NULL("IS NULL", (e1, e2) -> e1 + " AND " + e2),
            IS_NOT_NULL("IS NOT NULL", (e1, e2) -> e1 + " OR " + e2);

            private final String yql;
            private final BiFunction<String, String, String> exprCombiner;

            IsNullType(String yql, BiFunction<String, String, String> exprCombiner) {
                this.yql = yql;
                this.exprCombiner = exprCombiner;
            }

            public final String combine(String result, String element) {
                return result == null ? element : exprCombiner.apply(result, element);
            }
        }
    }

    private static final class AndPredicate extends YqlPredicate {
        private final List<YqlPredicate> predicates;

        private AndPredicate(Collection<YqlPredicate> predicates) {
            Preconditions.checkArgument(!predicates.isEmpty(), "Empty AND clause is disallowed");
            this.predicates = ImmutableList.copyOf(predicates);
        }

        @Override
        public Stream<YqlPredicateParam<?>> paramStream() {
            return predicates.stream().flatMap(YqlPredicate::paramStream);
        }

        @Override
        public YqlPredicate and(@NonNull YqlPredicate other) {
            if (other instanceof AndPredicate) {
                return new AndPredicate(ImmutableList.<YqlPredicate>builder()
                        .addAll(this.predicates)
                        .addAll(((AndPredicate) other).predicates)
                        .build());
            } else {
                return new AndPredicate(ImmutableList.<YqlPredicate>builder()
                        .addAll(this.predicates)
                        .add(other)
                        .build());
            }
        }

        @Override
        public <T extends Entity<T>> String toYql(@NonNull EntitySchema<T> schema) {
            if (predicates.size() == 1) {
                return predicates.get(0).toYql(schema);
            } else {
                return predicates.stream()
                        .map(p -> format("(%s)", p.toYql(schema)))
                        .collect(joining(" AND "));
            }
        }

        @Override
        public String toString() {
            return predicates.stream().map(p -> format("(%s)", p)).collect(joining(" && "));
        }
    }

    private static final class OrPredicate extends YqlPredicate {
        private final List<YqlPredicate> predicates;

        private OrPredicate(Collection<YqlPredicate> predicates) {
            Preconditions.checkArgument(!predicates.isEmpty(), "Empty OR clause is disallowed");
            this.predicates = ImmutableList.copyOf(predicates);
        }

        @Override
        public Stream<YqlPredicateParam<?>> paramStream() {
            return predicates.stream().flatMap(YqlPredicate::paramStream);
        }

        @Override
        public YqlPredicate or(@NonNull YqlPredicate other) {
            if (other instanceof OrPredicate) {
                return new OrPredicate(ImmutableList.<YqlPredicate>builder()
                        .addAll(this.predicates)
                        .addAll(((OrPredicate) other).predicates)
                        .build());
            } else {
                return new OrPredicate(ImmutableList.<YqlPredicate>builder()
                        .addAll(this.predicates)
                        .add(other)
                        .build());
            }
        }

        @Override
        public <T extends Entity<T>> String toYql(@NonNull EntitySchema<T> schema) {
            if (predicates.size() == 1) {
                return predicates.get(0).toYql(schema);
            } else {
                return predicates.stream()
                        .map(p -> format("(%s)", p.toYql(schema)))
                        .collect(joining(" OR "));
            }
        }

        @Override
        public String toString() {
            return predicates.stream().map(p -> format("(%s)", p)).collect(joining(" || "));
        }
    }

    private static final class NotPredicate extends YqlPredicate {
        private final YqlPredicate opposite;

        private NotPredicate(@NonNull YqlPredicate opposite) {
            this.opposite = opposite;
        }

        @Override
        public Stream<YqlPredicateParam<?>> paramStream() {
            return opposite.paramStream();
        }

        @Override
        public List<YqlPredicateParam<?>> paramList() {
            return opposite.paramList();
        }

        @Override
        public YqlPredicate negate() {
            return opposite;
        }

        @Override
        public <T extends Entity<T>> String toYql(@NonNull EntitySchema<T> schema) {
            return format("NOT (%s)", opposite.toYql(schema));
        }

        @Override
        public String toString() {
            return format("!(%s)", opposite);
        }
    }

    @RequiredArgsConstructor(access = PRIVATE)
    public static final class FieldPredicateBuilder {
        private final String fieldPath;
        private final UnaryOperator<YqlPredicate> finisher;

        @SafeVarargs
        public final <T> YqlPredicate in(@NonNull T possibleValue, @NonNull T... restOfPossibleValues) {
            return finisher.apply(YqlPredicate.in(fieldPath, possibleValue, restOfPossibleValues));
        }

        public <T> YqlPredicate in(@NonNull Collection<@NonNull ? extends T> values) {
            return finisher.apply(YqlPredicate.in(fieldPath, values));
        }

        @SafeVarargs
        public final <T> YqlPredicate notIn(@NonNull T possibleValue, @NonNull T... restOfPossibleValues) {
            return finisher.apply(YqlPredicate.notIn(fieldPath, possibleValue, restOfPossibleValues));
        }

        public <T> YqlPredicate notIn(@NonNull Collection<@NonNull ? extends T> values) {
            return finisher.apply(YqlPredicate.notIn(fieldPath, values));
        }

        public <T> YqlPredicate eq(@Nullable T value) {
            return finisher.apply(YqlPredicate.eq(fieldPath, value));
        }

        public <T> YqlPredicate neq(@Nullable T value) {
            return finisher.apply(YqlPredicate.neq(fieldPath, value));
        }

        public <T> YqlPredicate lt(@NonNull T value) {
            return finisher.apply(YqlPredicate.lt(fieldPath, value));
        }

        public <T> YqlPredicate lte(@NonNull T value) {
            return finisher.apply(YqlPredicate.lte(fieldPath, value));
        }

        public <T> YqlPredicate gt(@NonNull T value) {
            return finisher.apply(YqlPredicate.gt(fieldPath, value));
        }

        public <T> YqlPredicate gte(@NonNull T value) {
            return finisher.apply(YqlPredicate.gte(fieldPath, value));
        }

        public YqlPredicate like(@NonNull String value) {
            return like(value, null);
        }

        public YqlPredicate like(@NonNull String value, @Nullable Character escape) {
            return finisher.apply(YqlPredicate.like(fieldPath, value, escape));
        }

        public YqlPredicate notLike(@NonNull String value) {
            return notLike(value, null);
        }

        public YqlPredicate notLike(@NonNull String value, @Nullable Character escape) {
            return finisher.apply(YqlPredicate.notLike(fieldPath, value, escape));
        }

        public YqlPredicate isNull() {
            return finisher.apply(YqlPredicate.isNull(fieldPath));
        }

        public YqlPredicate isNotNull() {
            return finisher.apply(YqlPredicate.isNotNull(fieldPath));
        }
    }
}
