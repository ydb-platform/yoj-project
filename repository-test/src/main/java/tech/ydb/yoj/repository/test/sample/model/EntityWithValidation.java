package tech.ydb.yoj.repository.test.sample.model;

import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Value;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.Table;

import java.beans.ConstructorProperties;

@Getter
@EqualsAndHashCode
public class EntityWithValidation implements Entity<EntityWithValidation> {
    public static EntityWithValidation BAD_VALUE = new EntityWithValidation();
    public static EntityWithValidation BAD_VALUE_IN_VIEW = new EntityWithValidation(new Id("bad-value-view"), 45L);

    private final Id id;
    private final long value;

    @ConstructorProperties({"id", "value"})
    public EntityWithValidation(Id id, long value) {
        this.id = id;

        Preconditions.checkArgument(value != 42L, "Bad value: %s", value);
        this.value = value;
    }

    private EntityWithValidation() {
        this.id = new Id("bad-entity");
        this.value = 42L;
    }

    @Value
    public static class Id implements Entity.Id<EntityWithValidation> {
        String value;
    }

    @Getter
    @EqualsAndHashCode
    public static class OnlyVal implements Table.View {
        private final long value;

        @ConstructorProperties({"value"})
        public OnlyVal(long value) {
            Preconditions.checkArgument(value != 45L, "Bad value in view: %s", value);
            this.value = value;
        }
    }
}
