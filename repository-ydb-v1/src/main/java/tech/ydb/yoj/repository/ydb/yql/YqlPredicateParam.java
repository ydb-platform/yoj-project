package tech.ydb.yoj.repository.ydb.yql;

import com.google.common.base.Preconditions;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import tech.ydb.yoj.repository.ydb.statement.PredicateStatement;

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
    PredicateStatement.ComplexField complexField;
    PredicateStatement.CollectionKind collectionKind;

    /**
     * @return required predicate parameter
     */
    public static <T> YqlPredicateParam<T> of(String fieldPath, T value) {
        return of(fieldPath, value, false, PredicateStatement.ComplexField.FLATTEN, PredicateStatement.CollectionKind.SINGLE);
    }

    /**
     * @return optional predicate parameter
     */
    public static <T> YqlPredicateParam<T> optionalOf(String fieldPath, T value) {
        return of(fieldPath, value, true, PredicateStatement.ComplexField.FLATTEN, PredicateStatement.CollectionKind.SINGLE);
    }

    /**
     * @return required collection-valued predicate parameter
     */
    public static <V> YqlPredicateParam<Collection<V>> of(String fieldPath, Collection<V> coll) {
        return of(fieldPath, coll, false, PredicateStatement.ComplexField.FLATTEN, PredicateStatement.CollectionKind.DICT_SET);
    }

    /**
     * @return required collection-valued predicate parameter
     */
    @SafeVarargs
    public static <V> YqlPredicateParam<Collection<V>> of(String fieldPath, V first, V... rest) {
        return of(fieldPath, concat(Stream.of(first), stream(rest)).collect(toList()));
    }

    public static <T> YqlPredicateParam<T> of(String fieldPath, T value, boolean optional, PredicateStatement.ComplexField structKind, PredicateStatement.CollectionKind collectionKind) {
        if (value instanceof Collection<?>) {
            Preconditions.checkArgument(collectionKind != PredicateStatement.CollectionKind.SINGLE, "Collection parameter cannot be used with SINGLE collection kind");
            Preconditions.checkArgument(!optional, "Collection parameters cannot be optional");
            Preconditions.checkArgument(!((Collection<?>) value).isEmpty(), "Collection value must not be empty");
            return new YqlPredicateParam<>(fieldPath, value, false, structKind, collectionKind);
        } else {
            Preconditions.checkArgument(collectionKind == PredicateStatement.CollectionKind.SINGLE, "Non-collection parameters cannot be used with " + collectionKind + " collection kind");
            return new YqlPredicateParam<>(fieldPath, value, optional, structKind, collectionKind);
        }
    }
}
