//no package;

import tech.ydb.yoj.databind.schema.Table;
import tech.ydb.yoj.repository.db.Entity;

@Table(name = "table")
record TestRecord(String fieldOne, String field, Id id, InnerClass ic) implements Entity<TestRecord> {

    @Override
    public Entity.Id<TestRecord> getId() {
        return id;
    }

    record Id(String value) implements Entity.Id<TestRecord> {
    }

    static class InnerClass {
        String innerClassValue;
    }
}