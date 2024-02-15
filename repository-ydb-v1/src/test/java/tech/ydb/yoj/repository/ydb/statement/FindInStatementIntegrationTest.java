package tech.ydb.yoj.repository.ydb.statement;

import com.yandex.ydb.ValueProtos;
import lombok.NonNull;
import lombok.Value;
import org.junit.Test;
import tech.ydb.yoj.databind.DbType;
import tech.ydb.yoj.databind.expression.OrderExpression;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.GlobalIndex;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.Table;
import tech.ydb.yoj.repository.db.ViewSchema;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static tech.ydb.yoj.repository.db.EntityExpressions.defaultOrder;
import static tech.ydb.yoj.repository.db.EntityExpressions.newFilterBuilder;
import static tech.ydb.yoj.repository.db.EntityExpressions.newOrderBuilder;
import static tech.ydb.yoj.repository.db.EntityExpressions.orderById;

public class FindInStatementIntegrationTest {
    private static final EntitySchema<Foo> ENTITY_SCHEMA = EntitySchema.of(Foo.class);
    private static final Schema<FooView> VIEW_SCHEMA = ViewSchema.of(FooView.class);

    private static final Set<Foo.Id> IDS = Set.of(Foo.Id.of("1", "2"));
    private static final Set<FooIndexKey> KEYS = Set.of(
            FooIndexKey.of(null, "42", "2"), FooIndexKey.of(null, "17", "3")
    );
    private static final Set<FooIndexKey> INCONSISTENT_KEY_FIELDS = Set.of(
            FooIndexKey.of(null, null, "2"), FooIndexKey.of(null, "17", "2")
    );
    private static final Set<FooIndexKey> NOT_PREFIX_KEYS = Set.of(
            FooIndexKey.of(null, "42", null), FooIndexKey.of(null, "100500", null)
    );
    private static final Set<?> NOT_INDEXED_KEYS = Set.of(FooIndexKey.of(1L, null, null));
    private static final Set<?> INCONSISTENT_TYPE_KEYS = Set.of(FooIndexKeyInconsistentType.of(100500L));

    private static final OrderExpression<Foo> DEFAULT_ORDER = defaultOrder(Foo.class);

    private static final String INDEX_NAME = "index_by_value";
    private static final String NOT_EXISTENT_INDEX_NAME = "not_existent_index";

    @Test
    public void testGetQueryType() {
        var statement = new FindInStatement<>(ENTITY_SCHEMA, ENTITY_SCHEMA, IDS, null, null, null);

        assertThat(statement.getQueryType()).isEqualTo(Statement.QueryType.SELECT);
    }

    @Test
    public void testToDebugString() {
        var statement = new FindInStatement<>(ENTITY_SCHEMA, ENTITY_SCHEMA, IDS, null, null, null);

        assertThat(statement.toDebugString(IDS)).isNotBlank();
    }

    @Test
    public void testToDebugStringWithOrder() {
        var statement = new FindInStatement<>(ENTITY_SCHEMA, ENTITY_SCHEMA, IDS, null, DEFAULT_ORDER, null);

        assertThat(statement.toDebugString(IDS)).isNotBlank();
    }

    @Test
    public void testEntityUnordered() {
        var statement = new FindInStatement<>(ENTITY_SCHEMA, ENTITY_SCHEMA, IDS, null, null, null);

        String query = statement.getQuery("global/cloud/");
        String expected = """
                DECLARE $Input AS List<Struct<`key1`:Utf8,`key2`:Utf8>>;
                SELECT t.`key1` AS `key1`, t.`key2` AS `key2`, t.`value1` AS `value1`, t.`value2` AS `value2`
                FROM AS_TABLE($Input) AS k
                JOIN `global/cloud/FindInStatementIntegrationTest_Foo` AS t
                ON t.`key1` = k.`key1` AND t.`key2` = k.`key2`
                """;

        assertThat(query).isEqualTo(expected);
    }

    @Test
    public void testEntityDefaultOrder() {
        var statement = new FindInStatement<>(ENTITY_SCHEMA, ENTITY_SCHEMA, IDS, null, DEFAULT_ORDER, null);

        String query = statement.getQuery("global/cloud/");
        String expected = """
                DECLARE $Input AS List<Struct<`key1`:Utf8,`key2`:Utf8>>;
                SELECT t.`key1` AS `key1`, t.`key2` AS `key2`, t.`value1` AS `value1`, t.`value2` AS `value2`
                FROM AS_TABLE($Input) AS k
                JOIN `global/cloud/FindInStatementIntegrationTest_Foo` AS t
                ON t.`key1` = k.`key1` AND t.`key2` = k.`key2`
                ORDER BY `key1` ASC, `key2` ASC
                """;

        assertThat(query).isEqualTo(expected);
    }

    @Test
    public void testEntityCustomOrder() {
        var statement = new FindInStatement<>(ENTITY_SCHEMA, ENTITY_SCHEMA, IDS, null,
                newOrderBuilder(Foo.class)
                        .orderBy("id.key1").ascending()
                        .orderBy("value1").descending()
                        .build(),
                null
        );

        String query = statement.getQuery("global/cloud/");
        String expected = """
                DECLARE $Input AS List<Struct<`key1`:Utf8,`key2`:Utf8>>;
                SELECT t.`key1` AS `key1`, t.`key2` AS `key2`, t.`value1` AS `value1`, t.`value2` AS `value2`
                FROM AS_TABLE($Input) AS k
                JOIN `global/cloud/FindInStatementIntegrationTest_Foo` AS t
                ON t.`key1` = k.`key1` AND t.`key2` = k.`key2`
                ORDER BY `key1` ASC, `value1` DESC
                """;

        assertThat(query).isEqualTo(expected);
    }

    @Test
    public void testViewUnordered() {
        var statement = new FindInStatement<>(ENTITY_SCHEMA, VIEW_SCHEMA, IDS, null, null, null);

        String query = statement.getQuery("global/cloud/");
        String expected = """
                DECLARE $Input AS List<Struct<`key1`:Utf8,`key2`:Utf8>>;
                SELECT t.`key1` AS `key1`, t.`key2` AS `key2`, t.`value1` AS `value1`
                FROM AS_TABLE($Input) AS k
                JOIN `global/cloud/FindInStatementIntegrationTest_Foo` AS t
                ON t.`key1` = k.`key1` AND t.`key2` = k.`key2`
                """;

        assertThat(query).isEqualTo(expected);
    }

    @Test
    public void testViewDefaultOrder() {
        var statement = new FindInStatement<>(ENTITY_SCHEMA, VIEW_SCHEMA, IDS, null, DEFAULT_ORDER, null);

        String query = statement.getQuery("global/cloud/");
        String expected = """
                DECLARE $Input AS List<Struct<`key1`:Utf8,`key2`:Utf8>>;
                SELECT t.`key1` AS `key1`, t.`key2` AS `key2`, t.`value1` AS `value1`
                FROM AS_TABLE($Input) AS k
                JOIN `global/cloud/FindInStatementIntegrationTest_Foo` AS t
                ON t.`key1` = k.`key1` AND t.`key2` = k.`key2`
                ORDER BY `key1` ASC, `key2` ASC
                """;

        assertThat(query).isEqualTo(expected);
    }

    @Test
    public void testViewCustomOrder() {
        var statement = new FindInStatement<>(ENTITY_SCHEMA, VIEW_SCHEMA, IDS, null,
                newOrderBuilder(Foo.class)
                        .orderBy("id.key1").ascending()
                        .orderBy("value1").descending()
                        .build(),
                null
        );

        String query = statement.getQuery("global/cloud/");
        String expected = """
                DECLARE $Input AS List<Struct<`key1`:Utf8,`key2`:Utf8>>;
                SELECT t.`key1` AS `key1`, t.`key2` AS `key2`, t.`value1` AS `value1`
                FROM AS_TABLE($Input) AS k
                JOIN `global/cloud/FindInStatementIntegrationTest_Foo` AS t
                ON t.`key1` = k.`key1` AND t.`key2` = k.`key2`
                ORDER BY `key1` ASC, `value1` DESC
                """;

        assertThat(query).isEqualTo(expected);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testViewIncompatibleOrder() {
        var statement = new FindInStatement<>(ENTITY_SCHEMA, VIEW_SCHEMA, IDS, null,
                newOrderBuilder(Foo.class)
                        .orderBy("value2").descending()
                        .build(),
                null
        );

        statement.getQuery("global/cloud/");
    }

    @Test
    public void testFilter() {
        var filter = newFilterBuilder(Foo.class)
                .where("value1").neq(42L)
                .and("value2").in("v1", "v2")
                .build();
        var statement = new FindInStatement<>(ENTITY_SCHEMA, VIEW_SCHEMA, IDS, filter, DEFAULT_ORDER, null);

        String query = statement.getQuery("global/cloud/");
        String expected = """
                DECLARE $Input AS List<Struct<`key1`:Utf8,`key2`:Utf8>>;
                DECLARE $pred_0_value1 AS Int64;
                DECLARE $pred_1_value2 AS List<Utf8>;
                SELECT `key1`, `key2`, `value1`
                FROM (
                SELECT t.`key1` AS `key1`, t.`key2` AS `key2`, t.`value1` AS `value1`, t.`value2` AS `value2`
                FROM AS_TABLE($Input) AS k
                JOIN `global/cloud/FindInStatementIntegrationTest_Foo` AS t
                ON t.`key1` = k.`key1` AND t.`key2` = k.`key2`
                )
                WHERE (`value1` <> $pred_0_value1) AND (`value2` IN $pred_1_value2)
                ORDER BY `key1` ASC, `key2` ASC
                """;

        assertThat(query).isEqualTo(expected);
    }

    @Test
    public void testFilterOrderLimit() {
        var filter = newFilterBuilder(Foo.class)
                .where("value1").neq(42L)
                .and("value2").in("v1", "v2")
                .build();
        var statement = new FindInStatement<>(ENTITY_SCHEMA, VIEW_SCHEMA, IDS, filter, DEFAULT_ORDER, 42);

        String query = statement.getQuery("global/cloud/");
        String expected = """
                DECLARE $Input AS List<Struct<`key1`:Utf8,`key2`:Utf8>>;
                DECLARE $pred_0_value1 AS Int64;
                DECLARE $pred_1_value2 AS List<Utf8>;
                SELECT `key1`, `key2`, `value1`
                FROM (
                SELECT t.`key1` AS `key1`, t.`key2` AS `key2`, t.`value1` AS `value1`, t.`value2` AS `value2`
                FROM AS_TABLE($Input) AS k
                JOIN `global/cloud/FindInStatementIntegrationTest_Foo` AS t
                ON t.`key1` = k.`key1` AND t.`key2` = k.`key2`
                )
                WHERE (`value1` <> $pred_0_value1) AND (`value2` IN $pred_1_value2)
                ORDER BY `key1` ASC, `key2` ASC
                LIMIT 42
                """;

        assertThat(query).isEqualTo(expected);
    }

    @Test
    public void testByIndex() {
        var statement = new FindInStatement<>(
                ENTITY_SCHEMA,
                VIEW_SCHEMA,
                INDEX_NAME,
                KEYS,
                null, null, 1000
        );
        String query = statement.getQuery("global/cloud/");
        String expected = """
                DECLARE $Input AS List<Struct<`key2`:Utf8,`value2`:Utf8>>;
                SELECT t.`key1` AS `key1`, t.`key2` AS `key2`, t.`value1` AS `value1`
                FROM AS_TABLE($Input) AS k
                JOIN `global/cloud/FindInStatementIntegrationTest_Foo` VIEW `index_by_value` AS t
                ON t.`value2` = k.`value2` AND t.`key2` = k.`key2`
                LIMIT 1000
                """;

        assertThat(query).isEqualTo(expected);
    }

    @Test
    public void testNotExistentIndex() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new FindInStatement<>(
                        ENTITY_SCHEMA,
                        VIEW_SCHEMA,
                        NOT_EXISTENT_INDEX_NAME,
                        KEYS,
                        null, null, 1000
                ))
                .withMessageContaining(NOT_EXISTENT_INDEX_NAME);
    }

    @Test
    public void testByIndexInconsistentPrefixKeys() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new FindInStatement<>(
                        ENTITY_SCHEMA,
                        VIEW_SCHEMA,
                        INDEX_NAME,
                        INCONSISTENT_KEY_FIELDS,
                        null, null, 1000
                ));
    }

    @Test
    public void testByIndexNotPrefixKeys() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new FindInStatement<>(
                        ENTITY_SCHEMA,
                        VIEW_SCHEMA,
                        INDEX_NAME,
                        NOT_PREFIX_KEYS,
                        null, null, 1000
                ));
    }

    @Test
    public void testByIndexNotIndexedKey() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new FindInStatement<>(
                        ENTITY_SCHEMA,
                        VIEW_SCHEMA,
                        INDEX_NAME,
                        NOT_INDEXED_KEYS,
                        null, null, 1000
                ))
                .withMessageContaining("value1");
    }

    @Test
    public void testByIndexInconsistentTypeKeys() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new FindInStatement<>(
                        ENTITY_SCHEMA,
                        VIEW_SCHEMA,
                        INDEX_NAME,
                        INCONSISTENT_TYPE_KEYS,
                        null, null, 1000
                ))
                .withMessageContaining("java.lang.Long");
    }

    @Test
    public void testToQueryParameters() {
        var statement = new FindInStatement<>(ENTITY_SCHEMA, VIEW_SCHEMA, IDS, null, DEFAULT_ORDER, null);

        Map<String, ValueProtos.TypedValue> queryParams = statement.toQueryParameters(IDS);

        assertThat(queryParams.keySet()).containsOnly("$Input");
    }

    @Test
    public void testToQueryParametersWithFilter() {
        var filter = newFilterBuilder(Foo.class)
                .where("value1").neq(42L)
                .and("value2").in("v1", "v2")
                .build();
        var statement = new FindInStatement<>(ENTITY_SCHEMA, VIEW_SCHEMA, IDS, filter, DEFAULT_ORDER, null);

        Map<String, ValueProtos.TypedValue> queryParams = statement.toQueryParameters(IDS);

        assertThat(queryParams.keySet()).containsOnly("$Input", "$pred_0_value1", "$pred_1_value2");
    }

    @Test
    public void testEntityWithSimpleId() {
        var entitySchema = EntitySchema.of(Bar.class);
        var ids = List.of(Bar.Id.of("a"), Bar.Id.of("b"));
        var filter = newFilterBuilder(Bar.class).where("value").eq(100500L).build();
        var orderBy = orderById(Bar.class, OrderExpression.SortOrder.DESCENDING);
        var statement = new FindInStatement<>(entitySchema, entitySchema, ids, filter, orderBy, 42);

        String query = statement.getQuery("global/cloud/");
        String expected = """
                DECLARE $Input AS List<Struct<`idd`:Utf8>>;
                DECLARE $pred_0_value AS Int64;
                SELECT `idd`, `value`
                FROM (
                SELECT t.`idd` AS `idd`, t.`value` AS `value`
                FROM AS_TABLE($Input) AS k
                JOIN `global/cloud/FindInStatementIntegrationTest_Bar` AS t
                ON t.`idd` = k.`idd`
                )
                WHERE `value` = $pred_0_value
                ORDER BY `idd` DESC
                LIMIT 42
                """;

        assertThat(query).isEqualTo(expected);
    }

    @GlobalIndex(name = INDEX_NAME, fields = {
            "value2",
            "id.key2"
    })
    @Value
    static class Foo implements Entity<Foo> {
        @NonNull
        Id id;
        @Column(name = "value1", dbType = DbType.INT64)
        Long value1;
        @Column(name = "value2", dbType = DbType.UTF8)
        String value2;

        @Value(staticConstructor = "of")
        public static class Id implements Entity.Id<Foo> {
            @Column(name = "key1", dbType = DbType.UTF8)
            String key1;
            @Column(name = "key2", dbType = DbType.UTF8)
            String key2;
        }
    }

    @Value
    static class FooView implements Table.View {
        @NonNull
        Foo.Id id;
        @Column(name = "value1", dbType = DbType.UTF8)
        String value1;
    }

    @Value(staticConstructor = "of")
    static class FooIndexKey {
        @Column(name = "value1")
        Long v1;
        @Column(name = "key2")
        String k2;
        @Column(name = "value2")
        String v2;
    }

    @Value(staticConstructor = "of")
    static class FooIndexKeyInconsistentType {
        @Column(name = "value2")
        Long v2;
    }

    @Value
    static class Bar implements Entity<Bar> {
        @Column(name = "idd")
        @NonNull
        Id id;
        @Column(name = "value", dbType = DbType.INT64)
        Long value;

        @Value(staticConstructor = "of")
        public static class Id implements Entity.Id<Bar> {
            @Column(dbType = DbType.UTF8)
            String value;
        }
    }

}
