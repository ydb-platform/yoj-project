package tech.ydb.yoj.repository.db.json;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import tech.ydb.yoj.repository.db.common.JsonConverter;
import tech.ydb.yoj.repository.db.exception.ConversionException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.UnaryOperator;

/**
 * {@link JsonConverter YOJ JSON Converter} implementation using Jackson as the underlying JSON library.
 * Use it to support JSON-valued fields ({@link tech.ydb.yoj.databind.schema.Column @Column(flatten=false)} composite
 * objects and dynamic fields with type of interface/abstract class, e.g. {@link java.util.List}):
 * <blockquote>
 * <pre>
 * CommonConverters.defineJsonConverter(JacksonJsonConverter.getDefault());
 * </pre>
 * </blockquote>
 * <p><strong>Note that the {@code CommonConverters.defineJsonConverter()} configuration API is unstable and subject to
 * change (and potential deprecation for removal.)
 * </strong>
 * <p>
 * You can obtain an instance of {@link JacksonJsonConverter} in a number of ways:
 * <ul>
 * <li>To get {@link JacksonJsonConverter} which uses reasonable defaults, call {@link #getDefault()}.</li>
 * <li>To customize these reasonable defaults, use the {@link #JacksonJsonConverter(UnaryOperator)} constructor:
 * <blockquote>
 * <pre>
 * CommonConverters.defineJsonConverter(new JacksonJsonConverter(mapper -> mapper
 *     .set[...]()
 *     .registerModule(...)
 *     .configure(...)
 * ));
 * </pre>
 * </blockquote>
 * </li>
 * <li>To supply an externally created {@link ObjectMapper}, use the {@link #JacksonJsonConverter(ObjectMapper)}
 * constructor.</li>
 * </ul>
 */
@RequiredArgsConstructor
public final class JacksonJsonConverter implements JsonConverter {
    private static final JsonConverter instance = new JacksonJsonConverter(createDefaultObjectMapper());

    private final ObjectMapper mapper;

    public JacksonJsonConverter(UnaryOperator<ObjectMapper> mapperBuilder) {
        this(mapperBuilder.apply(createDefaultObjectMapper()));
    }

    /**
     * @return {@code JsonConverter} with reasonable defaults, using Jackson for JSON serialization and deserialization
     */
    public static JsonConverter getDefault() {
        return instance;
    }

    @Override
    public String toJson(@NonNull Type type, @Nullable Object o) throws ConversionException {
        try {
            return mapper.writerFor(mapper.getTypeFactory().constructType(type)).writeValueAsString(o);
        } catch (IOException e) {
            throw new ConversionException("Could not serialize an object of type `" + type + "` to JSON", e);
        }
    }

    @Override
    public <T> T fromJson(@NonNull Type type, @NonNull String content) throws ConversionException {
        try {
            return mapper.readerFor(mapper.getTypeFactory().constructType(type)).readValue(content);
        } catch (IOException e) {
            throw new ConversionException("Could not deserialize an object of type `" + type + "` from JSON", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T fromObject(@NonNull Type type, @Nullable Object content) throws ConversionException {
        try {
            JavaType jacksonType = mapper.getTypeFactory().constructType(type);
            return content != null
                    ? mapper.convertValue(content, jacksonType)
                    : (T) (jacksonType.isCollectionLikeType() ? List.of() : Map.of());
        } catch (Exception e) {
            throw new ConversionException("Could not convert an object to type `" + type + "`", e);
        }
    }

    public String toString() {
        return "JacksonJsonConverter";
    }

    /**
     * @return {@code ObjectMapper} with reasonable defaults for mapping between Java objects and JSON
     */
    public static ObjectMapper createDefaultObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setTimeZone(TimeZone.getDefault());
        mapper.registerModule(new Jdk8Module());
        mapper.registerModule(new JavaTimeModule());
        mapper.registerModule(new SimpleModule()
                .addAbstractTypeMapping(Set.class, LinkedHashSet.class)
                .addAbstractTypeMapping(Map.class, LinkedHashMap.class)
                .addAbstractTypeMapping(List.class, ArrayList.class)
        );
        mapper.setSerializationInclusion(Include.NON_NULL);
        mapper.configure(SerializationFeature.INDENT_OUTPUT, false);
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        mapper.configure(SerializationFeature.WRITE_DATE_KEYS_AS_TIMESTAMPS, false);
        mapper.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, false);
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.setVisibility(mapper.getSerializationConfig().getDefaultVisibilityChecker()
                .withFieldVisibility(Visibility.ANY)
                .withGetterVisibility(Visibility.NONE)
                .withIsGetterVisibility(Visibility.NONE)
                .withSetterVisibility(Visibility.NONE)
        );
        return mapper;
    }
}
