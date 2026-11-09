package tech.ydb.yoj.databind.schema;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Objects;

public interface ResourceEntity<R extends ResourceEntity<R>> {
    @Nonnull
    Attributes getAttributes();

    @Nonnull
    R withAttributes(@Nonnull Attributes meta);

    record Attributes(@Nonnull String name, @Nonnull Map<String, String> custom) {
        public Attributes {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(custom, "custom");
        }

        public Attributes(String name) {
            this(name, Map.of());
        }

        record NoCustom(String name) {
            public static NoCustom from(Attributes meta) {
                return new NoCustom(meta.name);
            }

            public Attributes toFull() {
                return new Attributes(this.name);
            }
        }
    }
}
