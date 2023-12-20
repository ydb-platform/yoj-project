package tech.ydb.yoj.repository.db.common;

import java.lang.reflect.Type;

public interface JsonConverter {
    String toJson(Type type, Object o);

    <T> T fromJson(Type type, String content);

    <T> T fromObject(Type type, Object content);

    JsonConverter NONE = new JsonConverter() {
        @Override
        public String toJson(Type type, Object o) {
            throw new UnsupportedOperationException("Define appropriate JSON converter!");
        }

        @Override
        public <T> T fromJson(Type type, String content) {
            throw new UnsupportedOperationException("Define appropriate JSON converter!");
        }

        @Override
        public <T> T fromObject(Type type, Object content) {
            throw new UnsupportedOperationException("Define appropriate JSON converter!");
        }

        @Override
        public String toString() {
            return "JsonConverter.NONE";
        }
    };
}
