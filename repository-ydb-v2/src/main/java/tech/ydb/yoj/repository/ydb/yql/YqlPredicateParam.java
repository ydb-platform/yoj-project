package tech.ydb.yoj.repository.ydb.yql;

import com.google.common.base.Preconditions;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import tech.ydb.yoj.repository.ydb.statement.PredicateStatement.CollectionKind;
import tech.ydb.yoj.repository.ydb.statement.PredicateStatement.ComplexField;

import java.util.Collection;
import java.util.stream.Stream;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;
import static lombok.AccessLevel.PRIVATE;

@Value
@RequiredArgsConstructor(access = PRIVATE)
public class YqlPredicateParam<T> {
    String fieldPath;
    T value;
    boolean optional;
    ComplexField complexField;
    CollectionKind collectionKind;

    public String getFieldPath() {
        Preconditions.checkState(complexField != ComplexField.EXPLICIT_TUPLE,
                "No single field path is available for explicit tuple parameters of PredicateStatement");
        return fieldPath;
    }

    /**
     * @return required predicate parameter
     */
    public static <T> YqlPredicateParam<T> of(String fieldPath, T value) {
        return of(fieldPath, value, false, ComplexField.FLATTEN, CollectionKind.SINGLE);
    }

    /**
     * @return optional predicate parameter
     */
    public static <T> YqlPredicateParam<T> optionalOf(String fieldPath, T value) {
        return of(fieldPath, value, true, ComplexField.FLATTEN, CollectionKind.SINGLE);
    }

    /**
     * @return required collection-valued predicate parameter
     */
    public static <V> YqlPredicateParam<Collection<V>> of(String fieldPath, Collection<V> coll) {
        return of(fieldPath, coll, false, ComplexField.FLATTEN, CollectionKind.DICT_SET);
    }

    /**
     * @return required collection-valued predicate parameter
     */
    @SafeVarargs
    public static <V> YqlPredicateParam<Collection<V>> of(String fieldPath, V first, V... rest) {
        return of(fieldPath, concat(Stream.of(first), stream(rest)).collect(toList()));
    }

    public static <T> YqlPredicateParam<T> of(String fieldPath, T value, boolean optional, ComplexField structKind, CollectionKind collectionKind) {
        if (value instanceof Collection<?>) {
            Preconditions.checkArgument(collectionKind != CollectionKind.SINGLE, "Collection parameter cannot be used with SINGLE collection kind");
            Preconditions.checkArgument(!optional, "Collection parameters cannot be optional");
            Preconditions.checkArgument(!((Collection<?>) value).isEmpty(), "Collection value must not be empty");
            return new YqlPredicateParam<>(fieldPath, value, false, structKind, collectionKind);
        } else {
            Preconditions.checkArgument(collectionKind == CollectionKind.SINGLE, "Non-collection parameters cannot be used with " + collectionKind + " collection kind");
            return new YqlPredicateParam<>(fieldPath, value, optional, structKind, collectionKind);
        }
    }

}
