package input.generated;

import javax.annotation.processing.Generated;

@Generated("tech.ydb.yoj.generator.FieldGeneratorAnnotationProcessor")
public final class EntityWithComplexSingularIdFields {
    private EntityWithComplexSingularIdFields(){}

    public static final String ID = "id";
    public static final String LAST_UPDATED = "lastUpdated";
    public static final String PAYLOAD = "payload";
    public static final String NOT_ID_OBJ = "notId";
    public static final class NotId {
        private NotId(){}

        public static final String NESTED_NOT_ID_OBJ = "notId.nestedNotId";
        public static final class NestedNotId {
            private NestedNotId(){}

            public static final String VALUE = "notId.nestedNotId.value";
        }
    }
}