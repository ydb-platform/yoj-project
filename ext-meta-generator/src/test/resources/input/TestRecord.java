//no package;
import tech.ydb.yoj.databind.schema.Table;

@Table(name="table")
record TestRecord(String fieldOne, String field, Id id, InnerClass ic) {

    record Id (String value){}

    static class InnerClass{
        String innerClassValue;
    }
}