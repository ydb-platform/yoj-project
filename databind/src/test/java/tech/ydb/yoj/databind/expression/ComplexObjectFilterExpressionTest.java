package tech.ydb.yoj.databind.expression;

import lombok.AllArgsConstructor;
import lombok.Value;
import org.junit.Test;
import tech.ydb.yoj.databind.converter.StringValueType;
import tech.ydb.yoj.databind.schema.ObjectSchema;
import tech.ydb.yoj.databind.schema.Schema;

import static org.assertj.core.api.Assertions.assertThat;

public class ComplexObjectFilterExpressionTest {
    private static final Schema<ObjA> schema = ObjectSchema.of(ObjA.class);

    @Test
    public void eqLevelOne() {
        FilterExpression<ObjA> f0 = FilterBuilder.forSchema(schema)
                .where("id").eq(new ObjA.ObjB(new ObjA.ObjC(2, 7), null, null))
                .build();
        assertThat(f0.toString()).isEqualTo("(id.id.x == 2) AND (id.id.y == 7) AND (id.x IS NULL) AND (id.y IS NULL)");

        FilterExpression<ObjA> f1 = FilterBuilder.forSchema(schema)
                .where("id").neq(new ObjA.ObjB(new ObjA.ObjC(2, 7), null, null))
                .build();
        assertThat(f1.toString()).isEqualTo("(id.id.x != 2) OR (id.id.y != 7) OR (id.x IS NOT NULL) OR (id.y IS NOT NULL)");
    }

    @Test
    public void eqLevelOneNull() {
        FilterExpression<ObjA> f0 = FilterBuilder.forSchema(schema)
                .where("id").eq(new ObjA.ObjB(null, 2, 7))
                .build();
        assertThat(f0.toString()).isEqualTo("(id.id.x IS NULL) AND (id.id.y IS NULL) AND (id.x == 2) AND (id.y == 7)");

        FilterExpression<ObjA> f1 = FilterBuilder.forSchema(schema)
                .where("id").neq(new ObjA.ObjB(null, 2, 7))
                .build();
        assertThat(f1.toString()).isEqualTo("(id.id.x IS NOT NULL) OR (id.id.y IS NOT NULL) OR (id.x != 2) OR (id.y != 7)");
    }

    @Test
    public void eqLevelTwo() {
        FilterExpression<ObjA> f0 = FilterBuilder.forSchema(schema)
                .where("id2").eq(new ObjA.ObjD(new ObjA.ObjB(new ObjA.ObjC(1, 2), 3, 4), 5, 6))
                .build();
        assertThat(f0.toString())
                .isEqualTo("(id2.id.id.x == 1) AND (id2.id.id.y == 2) AND (id2.id.x == 3) AND (id2.id.y == 4) AND (id2.x == 5) AND (id2.y == 6)");

        FilterExpression<ObjA> f1 = FilterBuilder.forSchema(schema)
                .where("id2").neq(new ObjA.ObjD(new ObjA.ObjB(new ObjA.ObjC(1, 2), 3, 4), 5, 6))
                .build();
        assertThat(f1.toString())
                .isEqualTo("(id2.id.id.x != 1) OR (id2.id.id.y != 2) OR (id2.id.x != 3) OR (id2.id.y != 4) OR (id2.x != 5) OR (id2.y != 6)");
    }

    @Test
    public void eqWithStringValueType() {
        var schema = ObjectSchema.of(SVTEntity.class);
        FilterExpression<SVTEntity> filter = FilterBuilder.forSchema(schema)
                .where("sub").eq(new SVTEntity.Sub("test a", new SVTEntity.SVTClass("test b")))
                .build();
        assertThat(filter.toString()).isEqualTo("(sub.a == \"test a\") AND (sub.b == \"test b\")");
    }

    @Test
    public void eqWithStringValueTypeLevelTwo() {
        var schema = ObjectSchema.of(SVTEntity.class);
        FilterExpression<SVTEntity> filter = FilterBuilder.forSchema(schema)
                .where("sub.b").eq(new SVTEntity.SVTClass("test b"))
                .build();
        assertThat(filter.toString()).isEqualTo("sub.b == \"test b\"");
    }

    @Value
    @AllArgsConstructor
    static class ObjA {
        ObjB id;

        ObjD id2;

        @Value
        @AllArgsConstructor
        public static class ObjB {
            ObjC id;

            Integer x;
            Integer y;
        }

        @Value
        @AllArgsConstructor
        public static class ObjC {
            Integer x;
            Integer y;
        }

        @Value
        @AllArgsConstructor
        public static class ObjD {
            ObjB id;

            Integer x;
            Integer y;
        }
    }

    @Value
    static class SVTEntity {
        Sub sub;

        @Value
        static class Sub {
            String a;
            SVTClass b;
        }

        @StringValueType
        record SVTClass(String value) {
            @Override
            public String toString() {
                return value;
            }
        }
    }
}
