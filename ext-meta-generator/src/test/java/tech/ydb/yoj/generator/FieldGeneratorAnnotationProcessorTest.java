package tech.ydb.yoj.generator;

import com.google.testing.compile.Compilation;
import com.google.testing.compile.Compiler;
import com.google.testing.compile.JavaFileObjects;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class FieldGeneratorAnnotationProcessorTest {

    @Test
    public void typicalEntity() throws IOException {
        testCase("input/TypicalEntity.java", "output/TypicalEntityFields.java");
    }

    @Test
    public void nestedEntity() throws IOException {
        testCase("input/NestedClass.java", "output/NestedClassFields.java");
    }

    @Test
    public void entityWithComplexNesting() throws IOException {
        testCase("input/ComplexNesting.java", "output/ComplexNestingFields.java");
    }

    @Test
    public void recordEntity() throws IOException {
        testCase("input/TestRecord.java", "output/TestRecordFields.java");
    }

    @Test
    public void unconventionalNaming() throws IOException {
        testCase("input/UnconventionalFieldNames.java", "output/UnconventionalFieldNamesFields.java");
    }

    @Test
    public void nonEntity() throws IOException {

        Compilation compilation = Compiler.javac()
                .withProcessors(new FieldGeneratorAnnotationProcessor())
                .compile(JavaFileObjects.forResource("input/NonEntityClass.java"));

        assertThat(compilation.errors()).isEmpty();
        assertThat(compilation.status()).isEqualTo(Compilation.Status.SUCCESS);
        assertThat(getGeneratedSource(compilation)).isEmpty();
    }

    private void testCase(String source, String expectations) throws IOException {

        Compilation compilation = Compiler.javac()
                .withProcessors(new FieldGeneratorAnnotationProcessor())
                .compile(JavaFileObjects.forResource(source));

        assertThat(compilation.errors()).isEmpty();
        assertThat(compilation.status()).isEqualTo(Compilation.Status.SUCCESS);
        assertThat(getGeneratedSource(compilation)).contains(getExpectations(expectations));
    }

    private String getExpectations(String fileName) throws IOException {
        try (
                Reader reader = JavaFileObjects.forResource(fileName).openReader(false);
                BufferedReader bufferedReader = new BufferedReader(reader)
        ) {
            return bufferedReader.lines().collect(Collectors.joining("\n"));
        }
    }

    private Optional<String> getGeneratedSource(Compilation compilation) throws IOException {
        if (compilation.generatedSourceFiles().isEmpty()) {
            return Optional.empty();
        }
        try (
                Reader reader = compilation.generatedSourceFiles().get(0).openReader(false);
                BufferedReader bufferedReader = new BufferedReader(reader)
        ) {
            return Optional.of(
                    bufferedReader.lines().collect(Collectors.joining("\n"))
            );
        }
    }
}
