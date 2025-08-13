package tech.ydb.yoj.databind.schema.naming;

import org.junit.Test;
import tech.ydb.yoj.databind.FieldValueType;
import tech.ydb.yoj.databind.schema.ObjectSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

public class MetaAnnotationTest {
    @Test
    public void basicMetaAnnotation() {
        var schema = ObjectSchema.of(MetaAnnotatedEntity.class);

        var idField = schema.getField("id");
        assertThat(idField.isRequired()).isTrue();

        var keyField = schema.getField("key");
        assertThat(keyField.getValueType()).isEqualTo(FieldValueType.OBJECT);

        var idColumn = idField.getField().getColumn();
        assertThat(idColumn).isNotNull();
        assertThat(idColumn.flatten()).isTrue();
        assertThat(idColumn.notNull()).isTrue();

        var keyColumn = keyField.getField().getColumn();
        assertThat(keyColumn).isNotNull();
        assertThat(keyColumn.flatten()).isFalse();
        assertThat(keyColumn.notNull()).isFalse();
    }

    @Test
    public void multipleAnnotationsNotAllowed() {
        assertThatIllegalArgumentException().isThrownBy(() -> ObjectSchema.of(BadMetaAnnotatedEntity.class));
    }
}
