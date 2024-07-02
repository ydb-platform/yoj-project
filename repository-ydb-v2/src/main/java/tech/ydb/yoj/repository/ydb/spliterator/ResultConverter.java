package tech.ydb.yoj.repository.ydb.spliterator;

import tech.ydb.proto.ValueProtos;

import java.util.List;

@FunctionalInterface
public interface ResultConverter<V> {
    V convert(List<ValueProtos.Column> columns, ValueProtos.Value value);
}
