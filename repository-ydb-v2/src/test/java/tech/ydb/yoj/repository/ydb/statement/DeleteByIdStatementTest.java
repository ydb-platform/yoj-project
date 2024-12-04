package tech.ydb.yoj.repository.ydb.statement;

import org.junit.Test;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.TableDescriptor;

import static org.assertj.core.api.Assertions.assertThat;

public class DeleteByIdStatementTest extends AbstractMultipleVarsYqlStatementTestBase {

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

            var schema = EntitySchema.of(TestEntity.class);
            var tableDescriptor = TableDescriptor.from(schema);
            var deleteStatement = new DeleteByIdStatement<>(tableDescriptor, schema);

            db.pendingExecute(deleteStatement, ENTITY_1.getId());
            db.pendingExecute(deleteStatement, ENTITY_3.getId());
        });

        var afterDelete = txManager.readOnly().run(() -> getTestEntityTable().findAll());

        assertThat(afterDelete).containsExactlyInAnyOrder(ENTITY_2);
    }
}
