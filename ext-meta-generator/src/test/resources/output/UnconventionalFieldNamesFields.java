package input.generated;

import javax.annotation.processing.Generated;

@Generated("tech.ydb.yoj.generator.FieldGeneratorAnnotationProcessor")
public final class UnconventionalFieldNamesFields {
    private UnconventionalFieldNamesFields(){}

    public static final String FIELD_ONE = "fieldOne";
    public static final String F_IELD_ONE = "fIeldOne";
    public static final String F_IELD_TWO = "f_ieldTwo";
    public static final String F__IELD_TWO = "f_IeldTwo";
    public static final String _FIE_LD_WITH_UNDERSCORES = "_fie_ldWithUnderscores";
    public static final String FIELD_IN__UPPER_CAMEL_CASE_OBJ = "FieldIn_UpperCamelCase";
    public static final class FieldIn_UpperCamelCase {
        private FieldIn_UpperCamelCase(){}

        public static final String NESTED_FIELD_IN_UPPER_CAMEL_CASE = "FieldIn_UpperCamelCase.NestedFieldInUpperCamelCase";
    }
    public static final String __FIELD_STARTED_WITH_UNDER_SCORE_OBJ = "_FieldStartedWithUnderScore";
    public static final class _FieldStartedWithUnderScore {
        private _FieldStartedWithUnderScore(){}

        public static final String NESTED_FIELD_IN_UPPER_CAMEL_CASE = "_FieldStartedWithUnderScore.NestedFieldInUpperCamelCase";
    }
}