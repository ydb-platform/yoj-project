package tech.ydb.yoj.repository.ydb;

import lombok.Value;
import org.junit.Test;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.ydb.statement.PredicateStatement;
import tech.ydb.yoj.repository.ydb.yql.YqlPredicate;
import tech.ydb.yoj.repository.ydb.yql.YqlPredicateParam;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.ydb.yoj.repository.ydb.yql.YqlPredicate.eq;
import static tech.ydb.yoj.repository.ydb.yql.YqlPredicate.gt;
import static tech.ydb.yoj.repository.ydb.yql.YqlPredicate.gte;
import static tech.ydb.yoj.repository.ydb.yql.YqlPredicate.iLike;
import static tech.ydb.yoj.repository.ydb.yql.YqlPredicate.in;
import static tech.ydb.yoj.repository.ydb.yql.YqlPredicate.like;
import static tech.ydb.yoj.repository.ydb.yql.YqlPredicate.lt;
import static tech.ydb.yoj.repository.ydb.yql.YqlPredicate.lte;
import static tech.ydb.yoj.repository.ydb.yql.YqlPredicate.neq;
import static tech.ydb.yoj.repository.ydb.yql.YqlPredicate.not;
import static tech.ydb.yoj.repository.ydb.yql.YqlPredicate.notILike;
import static tech.ydb.yoj.repository.ydb.yql.YqlPredicate.notLike;
import static tech.ydb.yoj.repository.ydb.yql.YqlPredicate.where;

public class YqlPredicateTest {
    private final EntitySchema<FakeEntity> schema = EntitySchema.of(FakeEntity.class);
    private final EntitySchema<FakeComplexEntity> complexSchema = EntitySchema.of(FakeComplexEntity.class);

    @Test
    public void in_fluent() {
        assertThat(where("status").in("DONE", "DONE_KEEP").toYql(schema)).isEqualToIgnoringCase("`status` IN ?");
    }

    @Test
    public void in_chained() {
        assertThat(in("status", "DONE", "DONE_KEEP").toYql(schema)).isEqualToIgnoringCase("`status` IN ?");
    }

    @Test
    public void rel_eq_fluent() {
        assertThat(where("workers").eq(42L).toYql(schema)).isEqualToIgnoringCase("`workers` = ?");
    }

    @Test
    public void rel_eq_chained() {
        assertThat(eq("workers", 42L).toYql(schema)).isEqualToIgnoringCase("`workers` = ?");
    }

    @Test
    public void rel_neq_fluent() {
        assertThat(where("workers").neq(42L).toYql(schema)).isEqualToIgnoringCase("`workers` <> ?");
    }

    @Test
    public void rel_neq_chained() {
        assertThat(neq("workers", 42L).toYql(schema)).isEqualToIgnoringCase("`workers` <> ?");
    }

    @Test
    public void rel_like_fluent() {
        assertThat(where("status").like("%OK%").toYql(schema)).isEqualToIgnoringCase("`status` LIKE ?");
    }

    @Test
    public void rel_like_chained() {
        assertThat(like("status", "%OK%").toYql(schema)).isEqualToIgnoringCase("`status` LIKE ?");
    }

    @Test
    public void rel_not_like_fluent() {
        assertThat(not(where("status").like("%OK%")).toYql(schema)).isEqualToIgnoringCase("`status` NOT LIKE ?");
    }

    @Test
    public void rel_not_like_chained() {
        assertThat(not(like("status", "%OK%")).toYql(schema)).isEqualToIgnoringCase("`status` NOT LIKE ?");
    }

    @Test
    public void rel_notLike_fluent() {
        assertThat(where("status").notLike("%OK%").toYql(schema)).isEqualToIgnoringCase("`status` NOT LIKE ?");
    }

    @Test
    public void rel_notLike_chained() {
        assertThat(notLike("status", "%OK%").toYql(schema)).isEqualToIgnoringCase("`status` NOT LIKE ?");
    }

    @Test
    public void rel_not_notLike_fluent() {
        assertThat(not(where("status").notLike("%OK%")).toYql(schema)).isEqualToIgnoringCase("`status` LIKE ?");
    }

    @Test
    public void rel_not_notLike_chained() {
        assertThat(not(notLike("status", "%OK%")).toYql(schema)).isEqualToIgnoringCase("`status` LIKE ?");
    }

    @Test
    public void rel_iLike_fluent() {
        assertThat(where("status").iLike("%OK%").toYql(schema)).isEqualToIgnoringCase("`status` ILIKE ?");
    }

    @Test
    public void rel_iLike_chained() {
        assertThat(iLike("status", "%OK%").toYql(schema)).isEqualToIgnoringCase("`status` ILIKE ?");
    }

    @Test
    public void rel_not_iLike_fluent() {
        assertThat(not(where("status").iLike("%OK%")).toYql(schema)).isEqualToIgnoringCase("`status` NOT ILIKE ?");
    }

    @Test
    public void rel_not_iLike_chained() {
        assertThat(not(iLike("status", "%OK%")).toYql(schema)).isEqualToIgnoringCase("`status` NOT ILIKE ?");
    }

    @Test
    public void rel_notILike_fluent() {
        assertThat(where("status").notILike("%OK%").toYql(schema)).isEqualToIgnoringCase("`status` NOT ILIKE ?");
    }

    @Test
    public void rel_notILike_chained() {
        assertThat(notILike("status", "%OK%").toYql(schema)).isEqualToIgnoringCase("`status` NOT ILIKE ?");
    }

    @Test
    public void rel_not_notILike_fluent() {
        assertThat(not(where("status").notILike("%OK%")).toYql(schema)).isEqualToIgnoringCase("`status` ILIKE ?");
    }

    @Test
    public void rel_not_notILike_chained() {
        assertThat(not(notILike("status", "%OK%")).toYql(schema)).isEqualToIgnoringCase("`status` ILIKE ?");
    }

    @Test
    public void rel_like_escape_fluent() {
        assertThat(where("status").like("%OK/_%", '/').toYql(schema)).isEqualToIgnoringCase("`status` LIKE ? ESCAPE '/'");
    }

    @Test
    public void rel_like_escape_chained() {
        assertThat(like("status", "%OK/_%", '/').toYql(schema)).isEqualToIgnoringCase("`status` LIKE ? ESCAPE '/'");
    }

    @Test
    public void rel_not_like_escape_fluent() {
        assertThat(not(where("status").like("%OK/_%", '/')).toYql(schema)).isEqualToIgnoringCase("`status` NOT LIKE ? ESCAPE '/'");
    }

    @Test
    public void rel_not_like_escape_chained() {
        assertThat(not(like("status", "%OK/_%", '/')).toYql(schema)).isEqualToIgnoringCase("`status` NOT LIKE ? ESCAPE '/'");
    }

    @Test
    public void rel_notLike_escape_fluent() {
        assertThat(where("status").notLike("%OK/_%", '/').toYql(schema)).isEqualToIgnoringCase("`status` NOT LIKE ? ESCAPE '/'");
    }

    @Test
    public void rel_notLike_escape_chained() {
        assertThat(notLike("status", "%OK/_%", '/').toYql(schema)).isEqualToIgnoringCase("`status` NOT LIKE ? ESCAPE '/'");
    }

    @Test
    public void rel_not_notLike_escape_fluent() {
        assertThat(not(where("status").notLike("%OK/_%", '/')).toYql(schema)).isEqualToIgnoringCase("`status` LIKE ? ESCAPE '/'");
    }

    @Test
    public void rel_not_notLike_escape_chained() {
        assertThat(not(notLike("status", "%OK/_%", '/')).toYql(schema)).isEqualToIgnoringCase("`status` LIKE ? ESCAPE '/'");
    }

    @Test
    public void rel_iLike_escape_fluent() {
        assertThat(where("status").iLike("%OK/_%", '/').toYql(schema)).isEqualToIgnoringCase("`status` ILIKE ? ESCAPE '/'");
    }

    @Test
    public void rel_iLike_escape_chained() {
        assertThat(iLike("status", "%OK/_%", '/').toYql(schema)).isEqualToIgnoringCase("`status` ILIKE ? ESCAPE '/'");
    }

    @Test
    public void rel_not_iLike_escape_fluent() {
        assertThat(not(where("status").iLike("%OK/_%", '/')).toYql(schema)).isEqualToIgnoringCase("`status` NOT ILIKE ? ESCAPE '/'");
    }

    @Test
    public void rel_not_iLike_escape_chained() {
        assertThat(not(iLike("status", "%OK/_%", '/')).toYql(schema)).isEqualToIgnoringCase("`status` NOT ILIKE ? ESCAPE '/'");
    }

    @Test
    public void rel_notILike_escape_fluent() {
        assertThat(where("status").notILike("%OK/_%", '/').toYql(schema)).isEqualToIgnoringCase("`status` NOT ILIKE ? ESCAPE '/'");
    }

    @Test
    public void rel_notILike_escape_chained() {
        assertThat(notILike("status", "%OK/_%", '/').toYql(schema)).isEqualToIgnoringCase("`status` NOT ILIKE ? ESCAPE '/'");
    }

    @Test
    public void rel_not_notILike_escape_fluent() {
        assertThat(not(where("status").notILike("%OK/_%", '/')).toYql(schema)).isEqualToIgnoringCase("`status` ILIKE ? ESCAPE '/'");
    }

    @Test
    public void rel_not_notILike_escape_chained() {
        assertThat(not(notILike("status", "%OK/_%", '/')).toYql(schema)).isEqualToIgnoringCase("`status` ILIKE ? ESCAPE '/'");
    }

    @Test
    public void rel_gt_fluent() {
        assertThat(where("workers").gt(42L).toYql(schema)).isEqualToIgnoringCase("`workers` > ?");
    }

    @Test
    public void rel_gt_chained() {
        assertThat(gt("workers", 42L).toYql(schema)).isEqualToIgnoringCase("`workers` > ?");
    }

    @Test
    public void rel_gte_fluent() {
        assertThat(where("workers").gte(42L).toYql(schema)).isEqualToIgnoringCase("`workers` >= ?");
    }

    @Test
    public void rel_gte_chained() {
        assertThat(gte("workers", 42L).toYql(schema)).isEqualToIgnoringCase("`workers` >= ?");
    }

    @Test
    public void rel_lt_fluent() {
        assertThat(where("workers").lt(42L).toYql(schema)).isEqualToIgnoringCase("`workers` < ?");
    }

    @Test
    public void rel_lt_chained() {
        assertThat(lt("workers", 42L).toYql(schema)).isEqualToIgnoringCase("`workers` < ?");
    }

    @Test
    public void rel_lte_fluent() {
        assertThat(where("workers").lte(42L).toYql(schema)).isEqualToIgnoringCase("`workers` <= ?");
    }

    @Test
    public void rel_lte_chained() {
        assertThat(lte("workers", 42L).toYql(schema)).isEqualToIgnoringCase("`workers` <= ?");
    }

    @Test
    public void chained_multiple_ands_are_merged() {
        // (workers > 0) AND (status = 'READY') AND (id = '<some random UUID>')
        YqlPredicate pred = gt("workers", 0L).and(eq("status", "READY")).and(eq("id", UUID.randomUUID()));
        assertThat(pred.toYql(schema)).isEqualToIgnoringCase("(`workers` > ?) AND (`status` = ?) AND (`id` = ?)");
    }

    @Test
    public void fluent_multiple_ands_are_merged() {
        // (workers > 0) AND (status = 'READY') AND (id = '<some random UUID>')
        YqlPredicate pred = where("workers").gt(0L).and("status").eq("READY").and("id").eq(UUID.randomUUID());
        assertThat(pred.toYql(schema)).isEqualToIgnoringCase("(`workers` > ?) AND (`status` = ?) AND (`id` = ?)");
    }

    @Test
    public void chained_multiple_ors_are_merged() {
        // (workers > 0) OR (status = 'READY') OR (id = '<some random UUID>')
        YqlPredicate pred = gt("workers", 0L).or(eq("status", "READY")).or(eq("id", UUID.randomUUID()));
        assertThat(pred.toYql(schema)).isEqualToIgnoringCase("(`workers` > ?) OR (`status` = ?) OR (`id` = ?)");
    }

    @Test
    public void fluent_multiple_ors_are_merged() {
        // (workers > 0) OR (status = 'READY') OR (id = '<some random UUID>')
        YqlPredicate pred = where("workers").gt(0L).or("status").eq("READY").or("id").eq(UUID.randomUUID());
        assertThat(pred.toYql(schema)).isEqualToIgnoringCase("(`workers` > ?) OR (`status` = ?) OR (`id` = ?)");
    }

    @Test
    public void fluent_builders_are_left_associative() {
        // ((workers > 0) AND (status = 'READY')) OR (status IN ('DONE', 'DONE_KEEP'))
        YqlPredicate pred = where("workers").gt(0L)
                .and("status").eq("READY")
                .or("status").in("DONE", "DONE_KEEP");

        assertThat(pred.toYql(schema)).isEqualToIgnoringCase("((`workers` > ?) AND (`status` = ?)) OR (`status` IN ?)");
        assertThat(pred.paramList()).containsExactly(
                YqlPredicateParam.of("workers", 0L, false, PredicateStatement.ComplexField.TUPLE, PredicateStatement.CollectionKind.SINGLE),
                YqlPredicateParam.of("status", "READY", false, PredicateStatement.ComplexField.TUPLE, PredicateStatement.CollectionKind.SINGLE),
                YqlPredicateParam.of(
                        "status",
                        List.of("DONE", "DONE_KEEP"),
                        false,
                        PredicateStatement.ComplexField.TUPLE,
                        PredicateStatement.CollectionKind.LIST
                ));
    }

    @Test
    public void generic_not() {
        // NOT ((status IN ('DONE', 'DONE_KEEP') AND (workers > 0))
        YqlPredicate pred = not(where("status").in("DONE", "DONE_KEEP").and("workers").gt(0L));

        assertThat(pred.toYql(schema)).isEqualToIgnoringCase("NOT ((`status` IN ?) AND (`workers` > ?))");
    }

    @Test
    public void generic_negate() {
        // NOT ((status IN ('DONE', 'DONE_KEEP') AND (workers > 0))
        YqlPredicate pred = where("status").in("DONE", "DONE_KEEP").and("workers").gt(0L).negate();

        assertThat(pred.toYql(schema)).isEqualToIgnoringCase("NOT ((`status` IN ?) AND (`workers` > ?))");
    }

    @Test
    public void in_not() {
        // status NOT IN ('DONE', 'DONE_KEEP')
        YqlPredicate pred = not(where("status").in("DONE", "DONE_KEEP"));

        assertThat(pred.toYql(schema)).isEqualToIgnoringCase("`status` NOT IN ?");
    }

    @Test
    public void in_negate() {
        // status NOT IN ('DONE', 'DONE_KEEP')
        YqlPredicate pred = where("status").in("DONE", "DONE_KEEP").negate();

        assertThat(pred.toYql(schema)).isEqualToIgnoringCase("`status` NOT IN ?");
    }

    @Test
    public void inComplex() {
        // id IN ('vla', 42)
        YqlPredicate pred = where("id").in(new YqlPredicateTest.FakeComplexEntity.Id("vla", 42));

        assertThat(pred.toYql(complexSchema)).isEqualToIgnoringCase("(`id_zone`, `id_localId`) IN ?");
    }

    @Test
    public void inComplex_negate() {
        // id NOT IN ('vla', 42)
        YqlPredicate pred = where("id").in(new YqlPredicateTest.FakeComplexEntity.Id("vla", 42))
                .negate();

        assertThat(pred.toYql(complexSchema)).isEqualToIgnoringCase("(`id_zone`, `id_localId`) NOT IN ?");
    }

    @Test
    public void rel_not() {
        // NOT(workers < 1) => workers >= 1
        YqlPredicate pred = not(where("workers").lt(1L));

        assertThat(pred.toYql(schema)).isEqualToIgnoringCase("`workers` >= ?");
    }

    @Test
    public void rel_negate() {
        // NOT(workers < 1) => workers >= 1
        YqlPredicate pred = where("workers").lt(1L).negate();

        assertThat(pred.toYql(schema)).isEqualToIgnoringCase("`workers` >= ?");
    }

    @Test
    public void double_not_is_idempotent() {
        // e.g. NOT(NOT ((workers > 1) AND (state IN('DONE', 'DONE_KEEP')))
        YqlPredicate origPred = where("workers").gt(1).and("status").in("DONE", "DONE_KEEP");
        YqlPredicate doubleNotPred = not(not(origPred));

        assertThat(origPred.toYql(schema)).isEqualToIgnoringCase(doubleNotPred.toYql(schema));
    }

    @Test
    public void double_negate_is_idempotent() {
        // e.g. NOT(NOT ((workers > 1) AND (state IN('DONE', 'DONE_KEEP')))
        YqlPredicate origPred = where("workers").gt(1).and("status").in("DONE", "DONE_KEEP");
        YqlPredicate doubleNotPred = origPred.negate().negate();

        assertThat(origPred.toYql(schema)).isEqualToIgnoringCase(doubleNotPred.toYql(schema));
    }

    @Test
    public void rel_eq_null_is_converted_to_IS_NULL() {
        assertThat(where("status").eq(null).toYql(schema)).isEqualToIgnoringCase("`status` IS NULL");
    }

    @Test
    public void rel_neq_null_is_converted_to_IS_NOT_NULL() {
        assertThat(where("status").neq(null).toYql(schema)).isEqualToIgnoringCase("`status` IS NOT NULL");
    }

    @Test(expected = RuntimeException.class)
    public void rel_other_rels_disallow_null() {
        where("status").gt(null);
    }

    @Test
    public void is_null() {
        assertThat(where("status").isNull().toYql(schema)).isEqualToIgnoringCase("`status` IS NULL");
    }

    @Test
    public void not_IS_NULL_becomes_IS_NOT_NULL() {
        assertThat(not(where("status").isNull()).toYql(schema)).isEqualToIgnoringCase("`status` IS NOT NULL");
    }

    @Test
    public void not_not_IS_NULL_becomes_IS_NULL_again() {
        assertThat(not(not(where("status").isNull())).toYql(schema)).isEqualToIgnoringCase("`status` IS NULL");
    }

    @Test
    public void is_not_null() {
        assertThat(where("status").isNotNull().toYql(schema)).isEqualToIgnoringCase("`status` IS NOT NULL");
    }

    @Test
    public void not_IS_NOT_NULL_becomes_IS_NULL() {
        assertThat(not(where("status").isNotNull()).toYql(schema)).isEqualToIgnoringCase("`status` IS NULL");
    }

    @Test
    public void not_not_IS_NOT_NULL_becomes_IS_NOT_NULL_again() {
        assertThat(not(not(where("status").isNotNull())).toYql(schema)).isEqualToIgnoringCase("`status` IS NOT NULL");
    }

    @Test
    public void complex_id_eq() {
        YqlPredicate pred = where("id").eq(new FakeComplexEntity.Id("ru-central-1", 100500L));

        assertThat(pred.toYql(complexSchema)).isEqualToIgnoringCase("(`id_zone`, `id_localId`) = ?");
    }

    @Test
    public void complex_id_neq_legacy() {
        YqlPredicate pred = where("id").neq(new FakeComplexEntity.Id("ru-central-1", 100500L));

        assertThat(pred.toYql(complexSchema)).isEqualToIgnoringCase("(`id_zone`, `id_localId`) <> ?");
    }

    @Test
    public void complex_id_less_and_greater_rels_are_allowed() {
        YqlPredicate predGt = where("id").gt(new FakeComplexEntity.Id("ru-central-1", 9000L));
        YqlPredicate predGte = where("id").gte(new FakeComplexEntity.Id("ru-central-1", 9000L));
        YqlPredicate predLt = where("id").lt(new FakeComplexEntity.Id("ru-central-1", 9000L));
        YqlPredicate predLte = where("id").lte(new FakeComplexEntity.Id("ru-central-1", 9000L));

        assertThat(predGt.toYql(complexSchema)).isEqualToIgnoringCase("(`id_zone`, `id_localId`) > ?");
        assertThat(predGte.toYql(complexSchema)).isEqualToIgnoringCase("(`id_zone`, `id_localId`) >= ?");
        assertThat(predLt.toYql(complexSchema)).isEqualToIgnoringCase("(`id_zone`, `id_localId`) < ?");
        assertThat(predLte.toYql(complexSchema)).isEqualToIgnoringCase("(`id_zone`, `id_localId`) <= ?");
    }

    @Test
    public void complex_id_is_null() {
        YqlPredicate pred = where("id").isNull();

        assertThat(pred.toYql(complexSchema)).isEqualToIgnoringCase("`id_zone` IS NULL AND `id_localId` IS NULL");
    }

    @Test
    public void not_complex_id_IS_NULL_is_transformed_to_IS_NOT_NULL() {
        YqlPredicate pred = not(where("id").isNull());

        assertThat(pred.toYql(complexSchema)).isEqualToIgnoringCase("`id_zone` IS NOT NULL OR `id_localId` IS NOT NULL");
    }

    @Test
    public void complex_id_is_not_null() {
        YqlPredicate pred = where("id").isNotNull();

        assertThat(pred.toYql(complexSchema)).isEqualToIgnoringCase("`id_zone` IS NOT NULL OR `id_localId` IS NOT NULL");
    }

    @Test
    public void not_complex_id_IS_NOT_NULL_is_transformed_to_IS_NULL() {
        YqlPredicate pred = not(where("id").isNotNull());

        assertThat(pred.toYql(complexSchema)).isEqualToIgnoringCase("`id_zone` IS NULL AND `id_localId` IS NULL");
    }

    @Value
    static class FakeEntity implements Entity<FakeEntity> {
        Id id;

        long workers;
        String status;

        @Value
        static class Id implements Entity.Id<FakeEntity> {
            long value2;
        }
    }

    @Value
    static class FakeComplexEntity implements Entity<FakeComplexEntity> {
        Id id;

        @Value
        static class Id implements Entity.Id<FakeComplexEntity> {
            String zone;
            long localId;
        }
    }
}
