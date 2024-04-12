## Goal
Generates a "-Fields" classes for all entities in a module.
This is useful for building queries like `where(AuditEvent.Id.TRAIL_ID).eq(trailId)`
Assume that we have an entity:
```java
     @Table
     class MyTable implements Entity<MyTable>{
         String strField;
         ComplexClass complexField;
         OtherClass fieldOfOtherClass;
         Id id;
         transient String aTransientField;
         
         static class ComplexClass {
             String value;
             OneMoreLevel deepField1;
             OneMoreLevel deepField2;

             static class OneMoreLevel {
                 Integer field;
             }
         }
         static class Id implements Entity.Id<MyTable> {
             String anyName;
         }
     }
```
The generated class will look like:
```java
//  Annotation processors must create a new class, so the name is different
final class MyTableFields { 
    private MyTableFields(){} // It must not be instantiated or inherited
    
    // Fields considered as 'simple' if their type is not another class nested into current one
    public static final String STR_FIELD = "strField";
    // `fieldOfOtherClass` is a simple field because `OtherClass` is not inside the given class
    public static final String FIELD_OF_OTHER_CLASS = "fieldOfOtherClass";

    
    // ComplexField is not simple because it has a type ComplexClass and there is a nested class ComplexClass
    // Pay attention that the generated nested class `ComplexField` uses the field's name! It's necessary because we might
    // have several fields of the same type

    // Alongside with the nested class, a 'simple' version of the field will be generated with suffix _OBJ
    // It allows to write queries like `where(MyTableFields.COMPLEX_FIELD_OBJ).eq(new ComplexClass(...))`
    public static final String COMPLEX_FIELD_OBJ = "complexField";
    
    // Aforementioned nested class
    public static class ComplexField {
        private IdField(){}

        // Mind that the value has "complexField." prefix
        public static final String VALUE = "complexField.value"; 

        // The annotation processor works recursively
        // Also it's the example of several fields with the same type
        public static final String DEEP_FIELD1_OBJ = "complexField.deepField1";
        public static final class DeepField1 {
            private DeepField1(){}

            public static final String FIELD = "complexField.deepField1.field";
        }
        
        public static final String DEEP_FIELD2_OBJ = "complexField.deepField2";
        public static final class DeepField2 {
            private DeepField2(){}

            public static final String FIELD = "complexField.deepField2.field";
        }
    }
    
    // a field of the `Entity.Id` type with a single field inside it is a special case. It will be collapsed into
    // a single field, just like the ORM expects. (even if there are several nested classes that can be reduced 
    // to a single field 
    public static final String ID = "id";
    
    // the transient field `aTransientField` is ignored because the ORM ignores transient fields
}
```
Additional info:
- Support Records
- Support Kotlin data classes

## Installation
Add an annotation processor to the compilation stage. 
- Example for Maven:
```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-compiler-plugin</artifactId>
    <version>3.11.0</version>
    <configuration>
        <annotationProcessorPaths>
            <annotationProcessorPath>
                <groupId>tech.ydb.yoj</groupId>
                <artifactId>yoj-ext-meta-generator</artifactId>
                <version>${yoj.version}</version>
            </annotationProcessorPath>
        </annotationProcessorPaths>
    </configuration>
</plugin>
```
- example for Gradle:
```kotlin
    dependencies {
        
        annotationProcessor("tech.ydb.yoj:yoj-ext-meta-generator:${yoj.version}")
    }
```

## Known issues

- if entity doesn't have `@Table` it won't be processed even if it's implements the `Entity` interface
- We assume that the annotation `@Table` is used on a top-level class
- The AP will break in case of two nested classes which refer each other (i.e. a circular dependency) 
- Will generate nested classes disregarding `@Column`'s flatten option 
- No logs are written
- if a field has the type of a class which is not nested inside the annotated class, the field will be ignored
- There is a rare situation when generated code won't compile. The following source:
```java
public final class Name {
     Class1 nameClash;
     public static final class Class1 {
         Class2 nameClash;
         public static final class Class2 {
             String value;
         }
     }
}
```
will produce
```java
public final class NameFields {
    public static final class NameClash {
        public static final class NameClash {
            public static final String VALUE = "nameClash.nameClash.value";
        }
    }
}
```
which won't compile due to 2 NameClash classes.
