package tech.ydb.yoj.repository.test.sample.model;

import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.With;
import tech.ydb.yoj.repository.db.Entity;

import java.time.Instant;

import static lombok.AccessLevel.PRIVATE;

@Value
@RequiredArgsConstructor(access = PRIVATE)
public class Bubble implements Entity<Bubble> {
    Id id;

    String fieldA;
    String fieldB;
    String fieldC;

    @With(PRIVATE)
    Instant updatedAt;

    public Bubble(Id id,  String fieldA, String fieldB, String fieldC) {
        this(id, fieldA, fieldB, fieldC, null);
    }

    @Override
    public Bubble preSave() {
        return withUpdatedAt(Instant.now());
    }

    @Value
    public static class Id implements Entity.Id<Bubble> {
        String a;
        String b;
    }
}
