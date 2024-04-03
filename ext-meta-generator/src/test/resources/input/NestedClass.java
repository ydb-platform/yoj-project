package package_name;

import tech.ydb.yoj.databind.schema.Table;

@Table(name = "audit_event_record")
public class NestedClass {

    String field;
    // Check that every complex field will generate its own nested class hierarchy
    // (See output/NestedClassFields.java)
    A a1;
    A a2;

    static class A {
        String fieldA;

        B b1;
        B b2;

        static class B{
            String fieldB1;
            String fieldB2;
        }
    }
}

