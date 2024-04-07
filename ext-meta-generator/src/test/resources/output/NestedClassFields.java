package package_name.generated;

import javax.annotation.processing.Generated;

@Generated("tech.ydb.yoj.generator.FieldGeneratorAnnotationProcessor")
public final class NestedClassFields {
    private NestedClassFields(){}

    public static final String FIELD = "field";
    public static final String A1_INTEGRAL = "a1";
    public static final class A1 {
        private A1(){}

        public static final String FIELD_A = "a1.fieldA";
        public static final String B1_INTEGRAL = "a1.b1";
        public static final class B1 {
            private B1(){}

            public static final String FIELD_B1 = "a1.b1.fieldB1";
            public static final String FIELD_B2 = "a1.b1.fieldB2";
        }
        public static final String B2_INTEGRAL = "a1.b2";
        public static final class B2 {
            private B2(){}

            public static final String FIELD_B1 = "a1.b2.fieldB1";
            public static final String FIELD_B2 = "a1.b2.fieldB2";
        }
    }
    public static final String A2_INTEGRAL = "a2";
    public static final class A2 {
        private A2(){}

        public static final String FIELD_A = "a2.fieldA";
        public static final String B1_INTEGRAL = "a2.b1";
        public static final class B1 {
            private B1(){}

            public static final String FIELD_B1 = "a2.b1.fieldB1";
            public static final String FIELD_B2 = "a2.b1.fieldB2";
        }
        public static final String B2_INTEGRAL = "a2.b2";
        public static final class B2 {
            private B2(){}

            public static final String FIELD_B1 = "a2.b2.fieldB1";
            public static final String FIELD_B2 = "a2.b2.fieldB2";
        }
    }
    public static final String C_INTEGRAL = "c";
    public static final class C {
        private C(){}

        public static final String D_INTEGRAL = "c.d";
        public static final class D {
            private D(){}

            public static final String FIELD_D = "c.d.fieldD";
        }
    }
}