package tech.ydb.yoj.databind.expression.visitor;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import tech.ydb.yoj.databind.expression.FilterExpression;
import tech.ydb.yoj.databind.expression.LeafExpression;

import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toList;
import static tech.ydb.yoj.databind.expression.FilterBuilder.and;
import static tech.ydb.yoj.databind.expression.FilterBuilder.not;
import static tech.ydb.yoj.databind.expression.FilterBuilder.or;

@RequiredArgsConstructor
public final class RemoveIf<T> extends FilterExpression.Visitor.Simple<T, FilterExpression<T>> {
    @NonNull
    private final Predicate<LeafExpression<T>> predicate;

    @Override
    protected FilterExpression<T> visitLeaf(@NonNull LeafExpression<T> leaf) {
        return predicate.test(leaf) ? null : leaf;
    }

    @Override
    protected FilterExpression<T> visitComposite(@NonNull FilterExpression<T> composite) {
        List<FilterExpression<T>> filtered = composite.stream()
                .map(e -> e.visit(this))
                .filter(Objects::nonNull)
                .collect(toList());
        if (filtered.isEmpty()) {
            return null;
        }

        return switch (composite.getType()) {
            case OR -> or(filtered);
            case AND -> and(filtered);
            case NOT -> not(filtered.get(0));
            default -> throw new UnsupportedOperationException("Unknown composite expression:" + composite);
        };
    }
}
