package tech.ydb.yoj.repository.ydb.spliterator;

import tech.ydb.proto.ValueProtos;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.yoj.repository.ydb.client.YdbConverter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public final class ResultSetIterator<V> implements Iterator<V> {
    private final ResultSetReader resultSet;
    private final ResultConverter<V> converter;
    private final List<ValueProtos.Column> columns;

    private int position = 0;

    public ResultSetIterator(ResultSetReader resultSet, ResultConverter<V> converter) {
        List<ValueProtos.Column> columns;
        if (resultSet.getRowCount() > 0) {
            resultSet.setRowIndex(0);
            columns = getColumns(resultSet);
        } else {
            columns = new ArrayList<>();
        }

        this.resultSet = resultSet;
        this.converter = converter;
        this.columns = columns;
    }

    @Override
    public boolean hasNext() {
        return position < resultSet.getRowCount();
    }

    @Override
    public V next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        ValueProtos.Value value = buildValue(position++);

        return converter.convert(columns, value);
    }

    private ValueProtos.Value buildValue(int rowIndex) {
        resultSet.setRowIndex(rowIndex);
        ValueProtos.Value.Builder value = ValueProtos.Value.newBuilder();
        for (int i = 0; i < columns.size(); i++) {
            value.addItems(YdbConverter.convertValueToProto(resultSet.getColumn(i)));
        }
        return value.build();
    }

    private static List<ValueProtos.Column> getColumns(ResultSetReader resultSet) {
        List<ValueProtos.Column> columns = new ArrayList<>();
        for (int i = 0; i < resultSet.getColumnCount(); i++) {
            columns.add(ValueProtos.Column.newBuilder()
                    .setName(resultSet.getColumnName(i))
                    .build()
            );
        }
        return columns;
    }

    @FunctionalInterface
    public interface ResultConverter<V> {
        V convert(List<ValueProtos.Column> columns, ValueProtos.Value value);
    }
}
