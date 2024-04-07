package input;

import tech.ydb.yoj.databind.schema.Table;

@Table(name = "table")
public class ComplexNesting {

    String field;
    // We 'jump over' Class1_2 to see if it's available and produce a correct result
    Class1.Class1_2.Class1_2_3 complex;
    Class1.Class1_2.EmptyClass emptyField;

    static class Class1 {
        Object someField; // Must ignore
        Class1_2 class1Field;  // Must Ignore

        static class Class1_2 {
            Object class2Field;
            EmptyClass emptyField;

            static class Class1_2_3 {
                Object class123Field;
                // important case: despite EmptyClass being defined outside of this class
                // it still available and must be treated as complex
                EmptyClass emptyField;
            }

            static class EmptyClass {

            }
        }
    }
}
