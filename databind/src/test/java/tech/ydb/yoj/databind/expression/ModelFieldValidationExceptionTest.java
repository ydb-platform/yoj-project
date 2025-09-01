package tech.ydb.yoj.databind.expression;

import lombok.Value;
import org.junit.Test;
import tech.ydb.yoj.databind.expression.values.StringFieldValue;
import tech.ydb.yoj.databind.schema.ObjectSchema;
import tech.ydb.yoj.databind.schema.Schema;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ModelFieldValidationExceptionTest {

    private static final Schema<Obj> schema = ObjectSchema.of(Obj.class);

    @Test
    public void shouldThrowUserException() {
        ModelField modelField = new ModelField("id", schema.getField("id"));
        assertThatThrownBy(() -> modelField.validateValue(new StringFieldValue("string")))
            .isExactlyInstanceOf(IllegalExpressionException.FieldTypeError.StringFieldExpected.class)
            .hasMessage("Type mismatch: cannot compare field \"id\" with a string value");
    }

    @Test
    public void shouldThrowInternalException() {
        ModelField modelField = ModelField.of(schema.getField("id"));
        assertThatThrownBy(() -> modelField.validateValue(new StringFieldValue("string")))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Specified a string value for non-string field \"id\"");
    }

    @Value
    private static class Obj {
        int id;
    }
}
