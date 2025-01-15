package tech.ydb.yoj.databind.schema.naming;

import lombok.NonNull;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.databind.schema.Table;

public class AnnotationFirstNamingStrategy implements NamingStrategy {
    public static final AnnotationFirstNamingStrategy instance = new AnnotationFirstNamingStrategy();

    @Override
    public String getNameForClass(@NonNull Class<?> entityClass) {
        var annotation = entityClass.getAnnotation(Table.class);
        if (annotation != null && !annotation.name().isEmpty()) {
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
        if (annotation != null && !annotation.name().isEmpty()) {
            var parentName = getParentAnnotationName(field);
            return (parentName == null || parentName.isEmpty())
                ? annotation.name()
                : parentName + NAME_DELIMITER + annotation.name();
        }

        return getColumnNameFromField(field);
    }

    private String getParentAnnotationName(Schema.JavaField field) {
        if (field.getParent() == null || field.getParent().getField() == null || field.getParent().getField().getColumn() == null)
            return null;
        return field.getParent().getField().getColumn().name();
    }

    protected String getColumnNameFromField(Schema.JavaField field) {
        return (field.getParent() == null)
                ? field.getField().getName()
                : field.getParent().getName() + NAME_DELIMITER + field.getField().getName();
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
