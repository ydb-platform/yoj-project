package tech.ydb.yoj.repository.ydb.statement;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import tech.ydb.yoj.repository.ydb.yql.YqlType;

@Getter
@AllArgsConstructor
@ToString
public class YqlStatementParam {
    YqlType type;
    String name;

    boolean optional;

    public static YqlStatementParam optional(YqlType type, String name) {
        return new YqlStatementParam(type, name, true);
    }

    public static YqlStatementParam required(YqlType type, String name) {
        return new YqlStatementParam(type, name, false);
    }

    public String getVar() {
        return "$" + name;
    }

    @Override
    public final int hashCode() {
        return name.hashCode();
    }

    @Override
    public final boolean equals(Object o) {
        return o instanceof YqlStatementParam && ((YqlStatementParam) o).name.equals(this.name);
    }
}
