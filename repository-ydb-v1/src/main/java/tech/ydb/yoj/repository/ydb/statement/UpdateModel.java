package tech.ydb.yoj.repository.ydb.statement;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import tech.ydb.yoj.repository.db.Entity.Id;
import tech.ydb.yoj.repository.db.statement.Changeset;
import tech.ydb.yoj.repository.ydb.yql.YqlPredicate;

import javax.annotation.Nullable;
import java.util.Map;

@Getter
@ToString
public abstract class UpdateModel {
    private final Map<String, ?> newValues;

    private UpdateModel(@NonNull Map<String, ?> newValues) {
        Preconditions.checkState(!newValues.isEmpty(),
                "update must contain a change to at least one entity field");
        this.newValues = newValues;
    }

    public static <T> Changeset set(String fieldPath, T value) {
        return new Changeset().set(fieldPath, value);
    }

    public static Changeset setAll(Map<String, ?> changes) {
        return new Changeset().setAll(changes);
    }

    public static Changeset setAll(Changeset changeset) {
        return new Changeset().setAll(changeset);
    }

    public boolean isEmpty() {
        return newValues.isEmpty();
    }

    @Nullable
    public Object getFieldValue(@NonNull String fieldPath) {
        return newValues.get(fieldPath);
    }

    @Getter
    public static final class ByPredicate extends UpdateModel {
        private final YqlPredicate predicate;

        /*package*/ ByPredicate(@NonNull YqlPredicate predicate, @NonNull Map<String, ?> newValues) {
            super(newValues);
            this.predicate = predicate;
        }
    }

    @Getter
    public static final class ById<ID extends Id<?>> extends UpdateModel {
        private final ID id;

        public ById(@NonNull ID id, @NonNull Map<String, ?> newValues) {
            super(newValues);
            this.id = id;
        }
    }

}
