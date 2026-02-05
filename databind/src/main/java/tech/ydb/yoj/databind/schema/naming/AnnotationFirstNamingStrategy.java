package tech.ydb.yoj.databind.schema.naming;

import com.google.common.base.Preconditions;
import lombok.NonNull;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.databind.schema.Table;

public class AnnotationFirstNamingStrategy implements NamingStrategy {
    public static final AnnotationFirstNamingStrategy instance = new AnnotationFirstNamingStrategy();

    @Override
    public String getNameForClass(@NonNull Class<?> entityClass) {
        var annotation = entityClass.getAnnotation(Table.class);
        if (annotation != null && !annotation.name().isEmpty()) {
            Preconditions.checkState(!annotation.explicitDescriptor(),
                    "Either use @Table(name=\"...\") or @Table(explicitDescriptor=true), but not both: <%s>",
                    entityClass.getSimpleName()
            );
            return annotation.name();
        }

        return getNameFromClass(entityClass);
    }

    @Override
    public void assignFieldName(@NonNull Schema.JavaField field) {
        field.setName(getColumnName(field));

        propagateFieldNameToFlatSubfield(field);
    }

    protected String getNameFromClass(Class<?> entityClass) {
        return entityClass.getName().replaceFirst(".*[.]", "").replace('$', '_');
    }

    private String getColumnName(Schema.JavaField field) {
        var annotation = field.getField().getColumn();
        var parentName = (field.getParent() == null) ? null : field.getParent().getName();

        if (annotation == null) {
            var fieldName = field.getField().getName();
            return parentName == null ? fieldName : parentName + NAME_DELIMITER + fieldName;
        }

        return getColumnNameWithNaming(field, annotation.name(), parentName, annotation.columnNaming());
    }

    private static String getColumnNameWithNaming(
        Schema.JavaField field,
        String annotationName,
        String parentName, Column.ColumnNaming columnNaming
    ) {
        var name = (annotationName.isEmpty()) ? field.getField().getName() : annotationName;

        return switch (columnNaming) {
            case ABSOLUTE -> name;
            case RELATIVE -> parentName + NAME_DELIMITER + name;
            case LEGACY -> {
                if (!annotationName.isEmpty()) {
                    yield annotationName;
                } else {
                    yield parentName == null ? name : parentName + NAME_DELIMITER + name;
                }
            }
        };
    }

    protected void propagateFieldNameToFlatSubfield(Schema.JavaField field) {
        if (field.isFlattenable() && field.isFlat()) {
            Schema.JavaField flatField = field.toFlatField();
            if (flatField.getName() == null) {
                flatField.setName(field.getName());
            }
        }
    }
}
