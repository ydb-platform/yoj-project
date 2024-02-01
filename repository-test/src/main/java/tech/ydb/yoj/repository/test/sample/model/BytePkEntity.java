package tech.ydb.yoj.repository.test.sample.model;

import tech.ydb.yoj.databind.ByteArray;
import tech.ydb.yoj.repository.db.RecordEntity;

public record BytePkEntity(
        Id id
) implements RecordEntity<BytePkEntity> {
    public record Id(
            ByteArray array
    ) implements RecordEntity.Id<BytePkEntity> {
    }

    public static BytePkEntity valueOf(int... array) {
        byte[] result = new byte[array.length];
        for (int i = 0; i < array.length; i++) {
            result[i] = (byte) array[i];
        }
        return new BytePkEntity(new Id(ByteArray.wrap(result)));
    }
}
