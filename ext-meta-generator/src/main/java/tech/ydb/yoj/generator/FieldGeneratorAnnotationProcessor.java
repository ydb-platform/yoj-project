package tech.ydb.yoj.generator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.databind.schema.Table;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.tools.FileObject;
import java.io.IOException;
import java.io.Writer;
import java.util.Set;

/**
 * Generates a "-Fields" classes for all entities in a module.
 * <p>This is useful for building queries like '<code>where(AuditEvent.Id.TRAIL_ID).eq(trailId)</code>'</p>
 * <p>Assume that we have an entity:</p>
 * <pre>{@code
 *     @Table(...)
 *     class MyTable {
 *         String strField;
 *         Object anyTypeField;
 *         Id idField;
 *         OtherClass fieldOfOtherClass;
 *
 *         static class Id {
 *             String value;
 *             OneMoreLevel deepField1;
 *             OneMoreLevel deepField2;
 *
 *             static class OneMoreLevel {
 *                 Integer field;
 *             }
 *         }
 *     }
 * }
 * A generated class will be
 *  <pre>{@code
 *      class MyTableFields { //  Annotation processors must create a new class, so the name is different
 *          // fields considered as 'simple' if there is no nested class of the field's type
 *          public static final String STR_FIELD = "strField";
 *          public static final String ANY_TYPE_FIELD = "anyTypeField";
 *          // `fieldOfOtherClass` is a simple field because `OtherClass` is not inside the given class
 *          public static final String FIELD_OF_OTHER_CLASS = "fieldOfOtherClass";
 *
 *          // idField is not simple because it has a type Id and there is a nested class Id
 *          // Pay attention that the generated nested class uses the field's name! It's necessary because we might
 *          // have several fields of the same type
 *          public static class IdField {
 *              public static final String VALUE = "idField.value"; // Mind that the value has "idField." prefix
 *
 *              // The Annotation Processor works recursively
 *              // Also it's the example of several fields with the same type
 *              static class DeepField1 {
 *                  public static final String FIELD = "idField.deepField1.field";
 *              }
 *              static class DeepField2 {
 *                  // Pay attention that ".deepField2." is used here
 *                  public static final String FIELD = "idField.deepField2.field";
 *              }
 *          }
 *      }
 *  }
 *  <ul>
 *      <b>Additional info:</b>
 *      <li>Support Records</li>
 *      <li>Support Kotlin data classes</li>
 *  </ul>
 * <ul>
 *      <b>Known issues (should be fixed in future):</b>
 *     <li>if entity doesn't have @Table it won't be processed even if it's implements Entity interface</li>
 *     <li>We assume that annotation @Table is used on top-level class</li>
 *     <li>The AP will break in case of two nested classes which refer each other (i.e. circular dependency) </li>
 *     <li>Will generate nested classes even if @Column(flatten=true) </li>
 *     <li>Will generate nested classes even if they are in fact empty </li>
 *     <li>No logs are written</li>
 *     <li>if a field has type of a class which is not nested inside the annotated class, the field will be ignored</li>
 *     <li>There is a rare situation when generated code won't compile. The following source class
 *     <pre>{@code
 *          class Name{
 *              Class1 nameClash;
 *              public static class Class1 {
 *                  Class2 nameClash;
 *                  class Class2{
 *                      String nameClash;
 *                  }
 *              }
 *          }
 *     }
 *     will produce
 *     <pre>{@code
 *         public class NameFields {
 *             public class NameClash {
 *                 public class NameClash {
 *                     public static final String NAME_CLASH = "nameClash.nameClash.nameClash";
 *                 }
 *             }
 *         }
 *     }
 *     which won't compile due to 2 NameClash classes.
 *     </li>
 * </ul>
 *
 * @author pavel-lazarev
 */

@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/pull/57")
@SupportedAnnotationTypes({
        "tech.ydb.yoj.databind.schema.Table",
})
@SupportedSourceVersion(SourceVersion.RELEASE_17)
public class FieldGeneratorAnnotationProcessor extends AbstractProcessor {

    private static final Logger log = LoggerFactory.getLogger(FieldGeneratorAnnotationProcessor.class);

    private static final String TARGET_PACKAGE = "generated";
    private static final String TARGET_CLASS_NAME_SUFFIX = "Fields";

    @Override
    public boolean process(Set<? extends TypeElement> set, RoundEnvironment roundEnvironment) {
        Set<? extends Element> elementsAnnotatedWith = roundEnvironment.getElementsAnnotatedWith(Table.class);
        log.info("Found {} classes to process ", elementsAnnotatedWith.size());
        for (Element rootElement : elementsAnnotatedWith) {
            log.debug("Processing {}", rootElement.getSimpleName());

            SourceClassStructure sourceClassStructure = SourceClassStructure.analyse(
                    rootElement,
                    processingEnv.getTypeUtils()
            );
            TargetClassStructure targetClassStructure = TargetClassStructure.build(
                    sourceClassStructure,
                    rootElement.getSimpleName() + TARGET_CLASS_NAME_SUFFIX
            );

            String packageName = Utils.concatFieldNameChain(
                    calcPackage(rootElement),
                    TARGET_PACKAGE
            );
            String generatedSource = StringConstantsRenderer.render(targetClassStructure, packageName);
            log.debug("Generated:\n {}", generatedSource);
            saveFile(
                    generatedSource,
                    Utils.concatFieldNameChain(
                            packageName,
                            targetClassStructure.className()
                    )
            );
        }

        return false;
    }

    private String calcPackage(Element element) {
        while (element.getKind() != ElementKind.PACKAGE) {
            element = element.getEnclosingElement();
        }
        PackageElement packageElement = (PackageElement) element;
        return packageElement.getQualifiedName().toString();
    }

    private void saveFile(String classContent, String fullClassName) {
        try {
            FileObject file = processingEnv.getFiler().createSourceFile(fullClassName);
            try (Writer writer = file.openWriter()) {
                writer.write(classContent);
            }
        } catch (IOException ex) {
            throw new RuntimeException(
                    "Can not save file %s. Content:\n%s".formatted(fullClassName, classContent),
                    ex
            );
        }
    }
}
