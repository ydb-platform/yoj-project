package tech.ydb.yoj.repository.test.sample.model;

import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.With;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.Table;

@Value
@AllArgsConstructor
public class Complex implements Entity<Complex> {
    Id id;
    String value;

    @Value
    @With
    public static class Id implements Entity.Id<Complex> {
        Integer a;
        Long b;
        String c;
        Status d;
    }

    public enum Status {
        OK, FAIL
    }

    public Complex(Id id) {
        this(id, "");
    }

    @Value
    public static class View implements Table.View {
        String value;
    }
}
