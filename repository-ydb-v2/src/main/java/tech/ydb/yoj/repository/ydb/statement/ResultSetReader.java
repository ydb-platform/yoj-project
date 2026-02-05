package tech.ydb.yoj.repository.ydb.statement;

import lombok.NonNull;
import tech.ydb.proto.ValueProtos;
import tech.ydb.yoj.InternalApi;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.repository.db.exception.ConversionException;
import tech.ydb.yoj.repository.ydb.yql.YqlType;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static tech.ydb.yoj.repository.db.EntityIdSchema.isIdFieldName;
import static tech.ydb.yoj.util.lang.BetterCollectors.toMapNullFriendly;

@InternalApi
public class ResultSetReader<RESULT> {
    private final Map<String, YqlType> fields;
    protected final Schema<RESULT> resultSchema;

    public ResultSetReader(@NonNull Schema<RESULT> resultSchema) {
        this.fields = resultSchema.flattenFields().stream().collect(toMap(Schema.JavaField::getName, YqlType::of));
        this.resultSchema = resultSchema;
    }

    public RESULT readResult(List<ValueProtos.Column> columnList, ValueProtos.Value value) {
        List<ValueProtos.Value> row = value.getItemsList();
        Map<String, Object> cells = IntStream.range(0, row.size()).boxed().collect(toMapNullFriendly(
                i -> columnList.get(i).getName(),
                i -> fields.get(columnList.get(i).getName()).fromYql(row.get(i)))
        );

        try {
            return resultSchema.newInstance(cells);
        } catch (Exception e) {
            throw new ConversionException(
                    format("Could not convert <%s> value %s: %s", resultSchema.getTypeName(), id(cells), e.getMessage()),
                    e
            );
        }
    }

    private static String id(Map<String, Object> cells) {
        return cells.entrySet().stream()
                .filter(e -> isIdFieldName(e.getKey()))
                .map(e -> format("%s=%s", e.getKey(), e.getValue()))
                .collect(joining(",", "{", "}"));
    }
}
