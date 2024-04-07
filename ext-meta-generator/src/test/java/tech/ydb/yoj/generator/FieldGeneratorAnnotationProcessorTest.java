package tech.ydb.yoj.generator;

import com.google.testing.compile.Compilation;
import com.google.testing.compile.Compiler;
import com.google.testing.compile.JavaFileObjects;
import com.tschuchort.compiletesting.KotlinCompilation;
import com.tschuchort.compiletesting.SourceFile;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class FieldGeneratorAnnotationProcessorTest {

    @Test
    public void typicalEntity() {
        testCase("input/TypicalEntity.java", "output/TypicalEntityFields.java");
    }

    @Test
    public void nestedClass() {
        testCase("input/NestedClass.java", "output/NestedClassFields.java");
    }

    @Test
    public void complexNesting() {
        testCase("input/ComplexNesting.java", "output/ComplexNestingFields.java");
    }

    @Test
    public void testRecord() {
        testCase("input/TestRecord.java", "output/TestRecordFields.java");
    }

    @Test
    public void unconventionalFieldNames() {
        testCase("input/UnconventionalFieldNames.java", "output/UnconventionalFieldNamesFields.java");
    }

    @Test
    public void entityWithComplexSingularId() {
        testCase("input/EntityWithComplexSingularId.java", "output/EntityWithComplexSingularIdFields.java");
    }

    @Test
    public void entityWithSimpleSingularId() {
        testCase("input/EntityWithSimpleSingularId.java", "output/EntityWithSimpleSingularIdFields.java");
    }

    @Test
    public void noSimpleFieldsClass() {
        testCase("input/NoSimpleFieldsClass.java", "output/NoSimpleFieldsClassFields.java");
    }

    @Test
    public void nonEntity() {

        Compilation compilation = Compiler.javac()
                .withProcessors(new FieldGeneratorAnnotationProcessor())
                .compile(JavaFileObjects.forResource("input/NonEntityClass.java"));

        assertThat(compilation.errors()).isEmpty();
        assertThat(compilation.status()).isEqualTo(Compilation.Status.SUCCESS);
        assertThat(getGeneratedSource(compilation)).isEmpty();
    }

    @Test
    public void kotlinSourceClass() {
        // Prepare
        SourceFile sourceFile = SourceFile.Companion.fromPath(new File("src/test/resources/input/KotlinDataClass.kt"));
        KotlinCompilation compilation = new KotlinCompilation();
        compilation.setAnnotationProcessors(List.of(
                new FieldGeneratorAnnotationProcessor()
        ));
        compilation.setSources(
                List.of(sourceFile)
        );
        compilation.setInheritClassPath(true);

        // Go
        KotlinCompilation.Result result = compilation.compile();

        // Asserts
        try {
            assertThat(result.getExitCode()).isEqualTo(KotlinCompilation.ExitCode.OK);
            assertThat(getGeneratedSource(result)).isEqualTo(getExpectations("output/KotlinDataClassFields.java"));
        } finally {
            result.getGeneratedFiles().forEach(File::deleteOnExit);
        }
    }

    private void testCase(String source, String expectations) {

        Compilation compilation = Compiler.javac()
                .withProcessors(new FieldGeneratorAnnotationProcessor())
                .compile(JavaFileObjects.forResource(source));

        assertThat(compilation.errors()).isEmpty();
        assertThat(compilation.status()).isEqualTo(Compilation.Status.SUCCESS);
        assertThat(getGeneratedSource(compilation)).contains(getExpectations(expectations));
    }

    private String getExpectations(String fileName) {
        try (
                Reader reader = JavaFileObjects.forResource(fileName).openReader(false);
                BufferedReader bufferedReader = new BufferedReader(reader)
        ) {
            return bufferedReader.lines().collect(Collectors.joining("\n"));
        } catch (Exception ex){
            throw new RuntimeException("Failed on file: "+fileName, ex);
        }
    }

    private String getGeneratedSource(KotlinCompilation.Result kotlinCompilation) {

        log.info("Total amount of generated files is {}", kotlinCompilation.getGeneratedFiles().size());
        List<String> generatedClasses = kotlinCompilation.getGeneratedFiles().stream()
                .map(File::getAbsolutePath)
                .filter(f -> f.endsWith("KotlinDataClassFields.java"))
                .toList();

        assertThat(generatedClasses).hasSize(1);
        try (BufferedReader reader = new BufferedReader(new FileReader(generatedClasses.get(0)))) {
            return reader.lines().collect(Collectors.joining("\n"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Optional<String> getGeneratedSource(Compilation compilation) {
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
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
