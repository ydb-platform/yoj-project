package package_name.generated;

import javax.annotation.processing.Generated;

@Generated("tech.ydb.yoj.generator.FieldGeneratorAnnotationProcessor")
public class NestedClassFields {
    public static final String FIELD = "field";
    public class A1 {
        public static final String FIELD_A = "a1.fieldA";
        public class B1 {
            public static final String FIELD_B1 = "a1.b1.fieldB1";
            public static final String FIELD_B2 = "a1.b1.fieldB2";
        }
        public class B2 {
            public static final String FIELD_B1 = "a1.b2.fieldB1";
            public static final String FIELD_B2 = "a1.b2.fieldB2";
        }
    }
    public class A2 {
        public static final String FIELD_A = "a2.fieldA";
        public class B1 {
            public static final String FIELD_B1 = "a2.b1.fieldB1";
            public static final String FIELD_B2 = "a2.b1.fieldB2";
        }
        public class B2 {
            public static final String FIELD_B1 = "a2.b2.fieldB1";
            public static final String FIELD_B2 = "a2.b2.fieldB2";
        }
    }
}