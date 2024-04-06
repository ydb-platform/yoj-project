package some_package.generated;

import javax.annotation.processing.Generated;

@Generated("tech.ydb.yoj.generator.FieldGeneratorAnnotationProcessor")
public final class NoSimpleFieldsClassFields {
    private NoSimpleFieldsClassFields(){}

    public static final class Field01 {
        private Field01(){}

        public static final class Field11 {
            private Field11(){}

            public class Field2 {
                private Field2(){}

            }
        }
        public static final class Field12 {
            private Field2(){}

        }
    }
    public static final class Field02 {
        private Field02(){}

        public static final class Field11 {
            private Field11(){}

            public static final class Field2 {
                private Field11(){}

            }
        }
        public static final class Field12 {
            private Field12(){}
        }
    }
}