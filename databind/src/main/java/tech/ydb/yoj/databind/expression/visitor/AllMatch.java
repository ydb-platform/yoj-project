package tech.ydb.yoj.databind.expression.visitor;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import tech.ydb.yoj.databind.expression.FilterExpression;
import tech.ydb.yoj.databind.expression.LeafExpression;

import java.util.function.Predicate;

@RequiredArgsConstructor
public final class AllMatch<T> extends FilterExpression.Visitor.Simple<T, Boolean> {
    @NonNull
    private final Predicate<LeafExpression<T>> predicate;

    @Override
    public Boolean visitLeaf(@NonNull LeafExpression<T> leaf) {
        return predicate.test(leaf);
    }

    @Override
    protected Boolean visitComposite(@NonNull FilterExpression<T> composite) {
        return composite.stream().allMatch(e -> e.visit(this));
    }
}
