package tech.ydb.yoj.databind.schema;

import lombok.Value;
import org.junit.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ChangefeedSchemaTest {
    @Test
    public void testChangefeedUndefinedEntity() {
        var entitySchema = schemaOf(SimpleEntity.class);
        assertThat(entitySchema.getChangefeeds()).isEmpty();
    }

    @Test
    public void testChangefeedDefaultsEntity() {
        var entitySchema = schemaOf(ChangefeedDefaultsEntity.class);

        assertThat(entitySchema.getChangefeeds()).hasSize(1);
        assertThat(entitySchema.getChangefeeds().get(0).getMode()).isEqualTo(Changefeed.Mode.NEW_IMAGE);
        assertThat(entitySchema.getChangefeeds().get(0).getFormat()).isEqualTo(Changefeed.Format.JSON);
        assertThat(entitySchema.getChangefeeds().get(0).getRetentionPeriod()).isEqualTo(Duration.ofHours(24));
        assertThat(entitySchema.getChangefeeds().get(0).isVirtualTimestamps()).isFalse();
        assertThat(entitySchema.getChangefeeds().get(0).isInitialScan()).isFalse();
        assertThat(entitySchema.getChangefeeds().get(0).getConsumers()).isEmpty();
    }

    @Test
    public void testZeroChangefeedNameEntity() {
        assertThatThrownBy(() -> schemaOf(ZeroChangefeedNameEntity.class));
    }

    @Test
    public void testConflictingChangefeedNameEntity() {
        assertThatThrownBy(() -> schemaOf(ConflictingChangefeedNameEntity.class));
    }

    @Test
    public void testPredefinedConsumersChangefeedEntity() {
        var entitySchema = schemaOf(PredefinedConsumersChangefeedEntity.class);

        assertThat(entitySchema.getChangefeeds()).hasSize(1);
        assertThat(entitySchema.getChangefeeds().get(0).getMode()).isEqualTo(Changefeed.Mode.NEW_IMAGE);
        assertThat(entitySchema.getChangefeeds().get(0).getFormat()).isEqualTo(Changefeed.Format.JSON);
        assertThat(entitySchema.getChangefeeds().get(0).getRetentionPeriod()).isEqualTo(Duration.ofHours(24));
        assertThat(entitySchema.getChangefeeds().get(0).isVirtualTimestamps()).isFalse();
        assertThat(entitySchema.getChangefeeds().get(0).isInitialScan()).isFalse();
        assertThat(entitySchema.getChangefeeds().get(0).getConsumers()).containsExactly("consumer1", "consumer2");
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
    @Changefeed(name = "feed1")
    private static class ChangefeedDefaultsEntity {
        int field1;
        int field2;
    }

    @Value
    @Changefeed(name = "")
    private static class ZeroChangefeedNameEntity {
        int field1;
        int field2;
    }

    @Value
    @Changefeed(name = "feed1")
    @Changefeed(name = "feed1")
    private static class ConflictingChangefeedNameEntity {
        int field1;
        int field2;
    }

    @Value
    @Changefeed(name = "feed1", consumers = {"consumer1", "consumer2"})
    private static class PredefinedConsumersChangefeedEntity {
        int field1;
        int field2;
    }
}
