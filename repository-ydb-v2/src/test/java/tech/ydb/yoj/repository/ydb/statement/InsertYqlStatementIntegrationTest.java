package tech.ydb.yoj.repository.ydb.statement;

import org.junit.Test;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.TableDescriptor;

import static org.assertj.core.api.Assertions.assertThat;

public class InsertYqlStatementIntegrationTest extends AbstractMultipleVarsYqlStatementIntegrationTestBase {
    @Test
    public void testInsert() {
        var txManager = getTxManager();

        txManager.tx(() -> getTestEntityTable().save(ENTITY_1));

        var beforeInsert = txManager.readOnly().run(() -> getTestEntityTable().findAll());

        assertThat(beforeInsert).containsExactlyInAnyOrder(ENTITY_1);

        txManager.tx(() -> {
            var db = getTestDb();

            var schema = EntitySchema.of(TestEntity.class);
            var tableDescriptor = TableDescriptor.from(schema);
            var deleteStatement = new InsertYqlStatement<>(tableDescriptor, schema);

            db.pendingExecute(deleteStatement, ENTITY_2);
            db.pendingExecute(deleteStatement, ENTITY_3);
        });

        var afterInsert = txManager.readOnly().run(() -> getTestEntityTable().findAll());

        assertThat(afterInsert).containsExactlyInAnyOrder(ENTITY_1, ENTITY_2, ENTITY_3);
    }
}
