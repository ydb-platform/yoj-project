package tech.ydb.yoj.repository.test.sample.model;

import lombok.Value;
import tech.ydb.yoj.repository.db.Entity;

import java.util.List;

@Value
public class Referring implements Entity<Referring> {
    Id id;
    Project.Id project;
    Complex.Id complex;
    List<Project.Id> projects;
    List<Complex.Id> complexes;

    @Value
    public static class Id implements Entity.Id<Referring> {
        String value;
    }
}
