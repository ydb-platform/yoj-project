package tech.ydb.yoj.repository.db.common;

import lombok.NonNull;
import tech.ydb.yoj.repository.db.exception.ConversionException;

import javax.annotation.Nullable;
import java.lang.reflect.Type;

public interface JsonConverter {
    /**
     * Serializes an object of the specified type to its JSON representation.
     *
     * @param type object type
     * @param o object to serialize (can be {@code null})
     * @return JSON representation of the serialized object, as a String
     * @throws ConversionException could not serialize
     * @throws UnsupportedOperationException serialization not supported
     */
    @NonNull
    String toJson(@NonNull Type type, @Nullable Object o)
            throws ConversionException, UnsupportedOperationException;

    /**
     * Deserializes an object of the specified type from its JSON representation.
     *
     * @param type object type
     * @param content JSON to deserialize, as a String
     * @return deserialized object (can be {@code null})
     * @throws ConversionException could not deserialize
     * @throws UnsupportedOperationException deserialization not supported
     */
    @Nullable
    <T> T fromJson(@NonNull Type type, @NonNull String content)
            throws ConversionException, UnsupportedOperationException;

    /**
     * Converts an object to a different type using JSON mapping logic as an intermediary.
     *
     * @param type type to convert the object to
     * @param content object to convert (can be {@code null})
     * @return converted object (can be {@code null})
     * @throws ConversionException could not convert
     * @throws UnsupportedOperationException conversion not supported
     */
    @Nullable
    <T> T fromObject(@NonNull Type type, @Nullable Object content)
            throws ConversionException, UnsupportedOperationException;

    /**
     * No-op JSON converter that just throws {@link UnsupportedOperationException} instead of trying to convert anything to/from JSON.
     */
    JsonConverter NONE = new JsonConverter() {
        @NonNull
        @Override
        public String toJson(@NonNull Type type, @Nullable Object o) {
            throw new UnsupportedOperationException("Define appropriate JSON converter!");
        }

        @Nullable
        @Override
        public <T> T fromJson(@NonNull Type type, @NonNull String content) {
            throw new UnsupportedOperationException("Define appropriate JSON converter!");
        }

        @Nullable
        @Override
        public <T> T fromObject(@NonNull Type type, @Nullable Object content) {
            throw new UnsupportedOperationException("Define appropriate JSON converter!");
        }

        @NonNull
        @Override
        public String toString() {
            return "JsonConverter.NONE";
        }
    };
}
