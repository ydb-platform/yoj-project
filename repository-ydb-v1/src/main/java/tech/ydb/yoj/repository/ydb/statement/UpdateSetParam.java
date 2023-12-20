package tech.ydb.yoj.repository.ydb.statement;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.ydb.yql.YqlType;

import java.util.stream.Stream;

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
