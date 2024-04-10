Add annotation process to the compilation stage. Example for maven:
```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-compiler-plugin</artifactId>
    <version>3.11.0</version>
    <configuration>
        <debug>true</debug>
        <annotationProcessorPaths>
            <annotationProcessorPath>
                <groupId>tech.ydb.yoj</groupId>
                <artifactId>yoj-ext-meta-generator</artifactId>
                <version>${yoj.version}</version>
            </annotationProcessorPath>
        </annotationProcessorPaths>
        <annotationProcessors>
            <annotationProcessor>tech.ydb.yoj.generator.FieldGeneratorAnnotationProcessor</annotationProcessor>
        </annotationProcessors>
    </configuration>
</plugin>
```
The annotation will process classes annotated with @Table such that
```java
package some.pack;

@Table(name = "audit_event_record")
public class TypicalEntity {
    @Column
    private final Id id;

    public static class Id {
        private final String topicName;
        private final int topicPartition;
        private final long offset;
    }
    @Nullable
    private final Instant lastUpdated;
}
```
and will generate meta-classes like:
```java
package some.pack.generated;

public class TypicalEntityFields {
    public static final String LAST_UPDATED = "lastUpdated";
    public class Id {
        public static final String TOPIC_NAME = "id.topicName";
        public static final String TOPIC_PARTITION = "id.topicPartition";
        public static final String OFFSET = "id.offset";
    }
}
```
