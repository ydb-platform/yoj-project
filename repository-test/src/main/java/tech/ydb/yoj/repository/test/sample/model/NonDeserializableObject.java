package tech.ydb.yoj.repository.test.sample.model;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.io.IOException;

@JsonDeserialize(using = NonDeserializableObject.Deserializer.class)
public final class NonDeserializableObject {
    static final class Deserializer extends JsonDeserializer<NonDeserializableObject> {
        @Override
        public NonDeserializableObject deserialize(JsonParser p,
                                                   DeserializationContext ctxt) throws IOException {
            throw new IOException("shit happened");
        }
    }
}
