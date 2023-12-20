package tech.ydb.yoj.databind.schema;

import lombok.Value;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TtlSchemaTest {
    private static final Duration TEST_TTL_INTERVAL = Duration.parse("PT12H");

    @Test
    public void testTtlUndefinedEntity() {
        var entitySchema = schemaOf(SimpleEntity.class);
        assertThat(entitySchema.getTtlModifier()).isNull();
    }

    @Test
    public void testTtlDefinedEntity() {
        var entitySchema = schemaOf(TtlEntity.class);

        assertThat(entitySchema.getTtlModifier()).isNotNull();
        assertThat(entitySchema.getTtlModifier().getFieldName()).isEqualTo("ttlColumn");
        assertThat(entitySchema.getTtlModifier().getInterval()).isEqualTo(TEST_TTL_INTERVAL.getSeconds());
    }

    @Test
    public void testTtlWrappedEntity() {
        var entitySchema = schemaOf(WrappedTtlEntity.class);

        assertThat(entitySchema.getTtlModifier()).isNotNull();
        assertThat(entitySchema.getTtlModifier().getFieldName()).isEqualTo("ttlEntity_ttlColumn");
        assertThat(entitySchema.getTtlModifier().getInterval()).isEqualTo(TEST_TTL_INTERVAL.getSeconds());
    }

    @Test
    public void testTtlNoColumnEntity() {
        assertThatThrownBy(() -> schemaOf(ErrorEntityNoColumn.class));
    }

    @Test
    public void testTtlBlankColumnEntity() {
        assertThatThrownBy(() -> schemaOf(ErrorEntityBlankColumn.class));
    }

    @Test
    public void testTtlComplexColumnEntity() {
        assertThatThrownBy(() -> schemaOf(ErrorEntityComplexColumn.class));
    }

    @Test
    public void testTtlInvalidIntervalEntity() {
        assertThatThrownBy(() -> schemaOf(ErrorEntityInvalidInterval.class));
    }

    @Test
    public void testNegativeTtlIntervalEntity() {
        assertThatThrownBy(() -> schemaOf(NegativeTtlEntity.class));
    }

    @Test
    public void testZeroTtlIntervalEntity() {
        assertThatThrownBy(() -> schemaOf(ZeroTtlEntity.class));
    }

    private static <T> Schema<T> schemaOf(Class<T> entityType) {
        return new TestSchema<>(entityType);
    }

    private static class TestSchema<T> extends Schema<T> {
        private TestSchema(Class<T> entityType) {
            super(entityType);
        }
    }

    @Value
    private static class SimpleEntity {
        int field1;
        int field2;
    }

    @Value
    @TTL(field = "ttlColumn", interval = "PT12H")
    private static class TtlEntity {
        Instant ttlColumn;
        String additionalData;
    }

    @Value
    @TTL(field = "wrongColumn", interval = "PT12H")
    private static class ErrorEntityNoColumn {
    }

    @Value
    @TTL(field = "", interval = "PT12H")
    private static class ErrorEntityBlankColumn {
    }

    @Value
    @TTL(field = "ttlColumn", interval = "PT12H")
    private static class ErrorEntityComplexColumn {
        SimpleEntity ttlColumn;
    }

    @Value
    @TTL(field = "ttlColumn", interval = "12H")
    private static class ErrorEntityInvalidInterval {
        Instant ttlColumn;
    }

    @Value
    @TTL(field = "ttlEntity.ttlColumn", interval = "PT12H")
    private static class WrappedTtlEntity {
        TtlEntity ttlEntity;
    }

    @Value
    @TTL(field = "ttlColumn", interval = "PT-12H")
    private static class NegativeTtlEntity {
        Instant ttlColumn;
    }

    @Value
    @TTL(field = "ttlColumn", interval = "PT0H")
    private static class ZeroTtlEntity {
        Instant ttlColumn;
    }
}
