package tech.ydb.yoj.repository.test.sample;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.SneakyThrows;
import tech.ydb.yoj.repository.db.common.CommonConverters;
import tech.ydb.yoj.repository.db.common.JsonConverter;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

/**
 * Test {@link JsonConverter} implementation that is sufficiently advanced to serialize and deserialize test data
 * used by YDB ORM tests.
 */
public final class TestJsonConverter implements JsonConverter {
    private static final JsonConverter instance = new TestJsonConverter();

    private static final ObjectMapper mapper = createObjectMapper();

    private TestJsonConverter() {
    }

    private static ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setTimeZone(TimeZone.getDefault());
        mapper.registerModule(new Jdk8Module());
        mapper.registerModule(new JavaTimeModule());
        mapper.registerModule(new SimpleModule()
                .addAbstractTypeMapping(Set.class, LinkedHashSet.class)
                .addAbstractTypeMapping(Map.class, LinkedHashMap.class)
                .addAbstractTypeMapping(List.class, ArrayList.class)
        );
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(SerializationFeature.INDENT_OUTPUT, false);
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        mapper.configure(SerializationFeature.WRITE_DATE_KEYS_AS_TIMESTAMPS, false);
        mapper.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true);
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
        mapper.setVisibility(mapper.getSerializationConfig().getDefaultVisibilityChecker()
                .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
                .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withIsGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
        );
        return mapper;
    }

    @Override
    @SneakyThrows
    public String toJson(Type type, Object o) {
        return mapper.writerFor(mapper.getTypeFactory().constructType(type)).writeValueAsString(o);
    }

    @Override
    @SneakyThrows
    public <T> T fromJson(Type type, String content) {
        return mapper.readerFor(mapper.getTypeFactory().constructType(type)).readValue(content);
    }

    @Override
    @SneakyThrows
    public Object fromObject(Type type, Object content) {
        JavaType jacksonType = mapper.getTypeFactory().constructType(type);
        if (content != null) {
            return mapper.convertValue(content, jacksonType);
        } else {
            return jacksonType.isCollectionLikeType() ? List.of() : Map.of();
        }
    }

    @Override
    public String toString() {
        return "TestJsonConverter.instance";
    }

    public static void register() {
        CommonConverters.defineJsonConverter(TestJsonConverter.instance);
    }
}
