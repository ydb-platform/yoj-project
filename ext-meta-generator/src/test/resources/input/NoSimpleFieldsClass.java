package some_package;

import tech.ydb.yoj.databind.schema.Table;

@Table(name="table")
class NoSimpleFieldsClass {

    Class1 field01;
    Class1 field02;

    private static class Class1{
        Class2 field11;
        Class2.Class3 field12;

        private static class IgnoreMe1 {
            String field;
        }

        private class Class2{
            Class3 field2;

            private static class Class3 {

            }
        }
    }

    private static class IgnoreMe0 {
        String field;
    }
}