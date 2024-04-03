package input;

import tech.ydb.yoj.databind.schema.Table;

@Table(name="table")
class UnconventionalFieldNames {

    //Trying to trick generator to generate the same name of constant
    String fieldOne;
    String fIeldOne;
    String f_ieldTwo;
    String f_IeldTwo;


    // Just some unconventional names to check generated source
    String _fie_ldWithUnderscores;
    strange_Name FieldIn_UpperCamelCase;
    strange_Name _FieldStartedWithUnderScore;
    class strange_Name{
        Object NestedFieldInUpperCamelCase;
    }
}