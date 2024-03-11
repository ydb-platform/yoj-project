package tech.ydb.yoj.repository.ydb.statement;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DeleteByIdStatementIntegrationTest extends AbstractMultipleVarsYqlStatementTestBase {

    @Test
    public void testDelete() {
        var txManager = getTxManager();

        txManager.tx(() -> {
            var table = getTestEntityTable();

            table.save(ENTITY_1);
            table.save(ENTITY_2);
            table.save(ENTITY_3);
        });

        var beforeDelete = txManager.readOnly().run(() -> getTestEntityTable().findAll());

        assertThat(beforeDelete).containsExactlyInAnyOrder(ENTITY_1, ENTITY_2, ENTITY_3);

        txManager.tx(() -> {
            var db = getTestDb();
            var deleteStatement = new DeleteByIdStatement<>(TestEntity.class);

            db.pendingExecute(deleteStatement, ENTITY_1.getId());
            db.pendingExecute(deleteStatement, ENTITY_3.getId());
        });

        var afterDelete = txManager.readOnly().run(() -> getTestEntityTable().findAll());

        assertThat(afterDelete).containsExactlyInAnyOrder(ENTITY_2);
    }
}
