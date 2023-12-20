package tech.ydb.yoj.repository.ydb;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;

import java.io.IOException;

@JsonSerialize(using = NonSerializableObject.Serializer.class)
final class NonSerializableObject {
    static final class Serializer extends JsonSerializer<NonSerializableObject> {
        @Override
        public void serializeWithType(NonSerializableObject value,
                                      JsonGenerator gen, SerializerProvider serializers,
                                      TypeSerializer typeSer) throws IOException {
            serialize(value, gen, serializers);
        }

        @Override
        public void serialize(NonSerializableObject value,
                              JsonGenerator gen, SerializerProvider serializers) throws IOException {
            throw new IOException("shit happened");
        }
    }
}
