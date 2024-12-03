package tech.ydb.yoj.repository.ydb.statement;

import org.junit.Test;
import tech.ydb.yoj.repository.db.EntitySchema;

import static org.assertj.core.api.Assertions.assertThat;

public class UpsertYqlStatementTest extends AbstractMultipleVarsYqlStatementTestBase {

    @Test
    public void testUpsert() {
        var txManager = getTxManager();

        txManager.tx(() -> getTestEntityTable().save(ENTITY_1));

        var beforeUpsert = txManager.readOnly().run(() -> getTestEntityTable().findAll());

        assertThat(beforeUpsert).containsExactlyInAnyOrder(ENTITY_1);

        txManager.tx(() -> {
            var db = getTestDb();
            var deleteStatement = new UpsertYqlStatement<>(EntitySchema.of(TestEntity.class));

            db.pendingExecute(deleteStatement, ENTITY_1_1);
            db.pendingExecute(deleteStatement, ENTITY_2);
            db.pendingExecute(deleteStatement, ENTITY_3);
        });

        var afterUpsert = txManager.readOnly().run(() -> getTestEntityTable().findAll());

        assertThat(afterUpsert).containsExactlyInAnyOrder(ENTITY_1_1, ENTITY_2, ENTITY_3);
    }
}
