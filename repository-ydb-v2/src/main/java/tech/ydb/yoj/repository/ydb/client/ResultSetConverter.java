package tech.ydb.yoj.repository.ydb.client;

import tech.ydb.proto.ValueProtos;
import tech.ydb.table.result.ResultSetReader;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ResultSetConverter {
    private final ResultSetReader resultSet;
    private List<ValueProtos.Column> columns = new ArrayList<>();

    public ResultSetConverter(ResultSetReader resultSet) {
        this.resultSet = resultSet;

        if (resultSet.getRowCount() > 0) {
            columns = getColumns(resultSet);
        }
    }

    public <RESULT> Stream<RESULT> stream(BiFunction<List<ValueProtos.Column>, ValueProtos.Value, RESULT> mapper) {
        return IntStream.range(0, resultSet.getRowCount())
                .mapToObj(this::buildValue)
                .map(value -> mapper.apply(columns, value));
    }

    private ValueProtos.Value buildValue(int rowIndex) {
        resultSet.setRowIndex(rowIndex);
        ValueProtos.Value.Builder value = ValueProtos.Value.newBuilder();
        for (int col = 0; col < columns.size(); col++) {
            value.addItems(YdbConverter.convertValueToProto(resultSet.getColumn(col)));
        }
        return value.build();
    }

    private static List<ValueProtos.Column> getColumns(ResultSetReader resultSet) {
        resultSet.setRowIndex(0);
        List<ValueProtos.Column> result = new ArrayList<>();
        for (int i = 0; i < resultSet.getColumnCount(); i++) {
            result.add(ValueProtos.Column.newBuilder()
                    .setName(resultSet.getColumnName(i))
                    .build());
        }
        return result;
    }
}
