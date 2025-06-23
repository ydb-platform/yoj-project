package tech.ydb.yoj.repository.ydb.statement;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.ydb.yql.YqlType;

import java.util.stream.Stream;

/**
 * @deprecated Blindly setting entity fields is not recommended. Use {@code Table.modifyIfPresent()} instead, unless you
 * have specific requirements.
 * <p>Blind updates disrupt query merging mechanism, so you typically won't able to run multiple blind update statements
 * in the same transaction, or interleave them with upserts ({@code Table.save()}) and inserts.
 * <p>Blind updates also do not update projections because they do not load the entity before performing the update;
 * this can cause projections to be inconsistent with the main entity.
 */
@Deprecated
@Getter
public final class UpdateSetParam extends PredicateStatement.Param {
    private static final String PREFIX = "set_";

    private final EntitySchema.JavaField field;

    private final EntitySchema.JavaField rootField;
    private final String rootFieldPath;

    private UpdateSetParam(@NonNull EntitySchema.JavaField field,
                           @NonNull EntitySchema.JavaField rootField, @NonNull String rootFieldPath) {
        super(YqlType.of(field), PREFIX + underscoreIllegalSymbols(field.getName()), true);
        Preconditions.checkState(field.isFlat(), "Can only create update statements for flat fields");
        this.field = field;

        this.rootField = rootField;
        this.rootFieldPath = rootFieldPath;
    }

    static Stream<UpdateSetParam> setParamsFromModel(@NonNull EntitySchema<?> schema, @NonNull UpdateModel model) {
        return model.getNewValues().keySet().stream().flatMap(rootFieldPath -> {
            EntitySchema.JavaField rootField = schema.getField(rootFieldPath);
            return rootField.flatten().map(jf -> new UpdateSetParam(jf, rootField, rootFieldPath));
        });
    }

    String getFieldName() {
        return field.getName();
    }

    public Object getFieldValue(@NonNull UpdateModel model) {
        return getParamValue(rootField, getFieldName(), false, model.getNewValues().get(rootFieldPath));
    }
}
