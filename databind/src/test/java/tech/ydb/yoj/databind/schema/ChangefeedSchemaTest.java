package tech.ydb.yoj.databind.schema;

import lombok.Value;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

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

        Schema.Changefeed expectedChangefeed = new Schema.Changefeed(
                "feed1",
                Changefeed.Mode.NEW_IMAGE,
                Changefeed.Format.JSON,
                false,
                Duration.ofHours(24),
                false,
                List.of(
                        new Schema.Changefeed.Consumer(
                                "consumer1",
                                List.of(),
                                Instant.EPOCH,
                                false
                        ),
                        new Schema.Changefeed.Consumer(
                                "consumer2",
                                List.of(Changefeed.Consumer.Codec.RAW),
                                Instant.parse("2020-01-01T00:00:00Z"),
                                true
                        )
                )
        );

        assertThat(entitySchema.getChangefeeds())
                .singleElement()
                .isEqualTo(expectedChangefeed);
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
    @Changefeed(name = "feed1", consumers = {
            @Changefeed.Consumer(name = "consumer1"),
            @Changefeed.Consumer(
                    name = "consumer2",
                    readFrom = "2020-01-01T00:00:00Z",
                    codecs = {Changefeed.Consumer.Codec.RAW},
                    important = true
            )
    })
    private static class PredefinedConsumersChangefeedEntity {
        int field1;
        int field2;
    }
}
