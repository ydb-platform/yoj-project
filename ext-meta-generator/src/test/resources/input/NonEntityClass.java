package yandex.cloud.trail.model;

import java.time.Instant;
import java.util.Comparator;

import javax.annotation.Nullable;
import javax.annotation.Nonnull;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.Table; // imported but not used

// We should just ignore this class
public class NonEntityClass {

    public static final Comparator<NonEntityClass> COMPARE_BY_ID =
            (e1, e2) -> NonEntityClass.Id.NATURAL_ORDER.compare(e1.getId(), e2.getId());

    @Column
    @Nonnull private final Id id;

    public static class Id {

        private static final Comparator<Id> NATURAL_ORDER =
                Comparator.comparing(Id::getTrailId)
                        .thenComparing(Id::getTopicName)
                        .thenComparingInt(Id::getTopicPartition)
                        .thenComparingLong(Id::getOffset);

        public Id(@Nonnull String trailId, @Nonnull String topicName, int topicPartition, long offset) {
            this.trailId = trailId;
            this.topicName = topicName;
            this.topicPartition = topicPartition;
            this.offset = offset;
        }

        @Nonnull
        private final String trailId;

        @Nonnull
        private final String topicName;

        private final int topicPartition;

        private final long offset;

        public String getTrailId() {
            return trailId;
        }

        public String getTopicName() {
            return topicName;
        }

        public int getTopicPartition() {
            return topicPartition;
        }

        public long getOffset() {
            return offset;
        }
    }

    public NonEntityClass(Id id, Instant lastUpdated, Object payload, Object metadata, SomeEnum state) {
        this.id = id;
        this.lastUpdated = lastUpdated;
        this.payload = payload;
        this.metadata = metadata;
        this.state = state;
    }

    @Nullable
    private final Instant lastUpdated;

    @Nullable
    private final Object payload;

    @Nullable
    private final Object metadata;

    private final SomeEnum state;

    public yandex.cloud.trail.model.NonEntityClass.Id getId() {
        return id;
    }

    enum SomeEnum {
        WOW, THIS, IS, A, COOL, ENUM;
    }
}
