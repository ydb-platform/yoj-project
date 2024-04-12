package tech.ydb.yoj.generator;

import com.google.auto.service.AutoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.yoj.ExperimentalApi;
import tech.ydb.yoj.databind.schema.Table;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Processor;
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
 * See README.md in the root of the module
 * @author lazarev-pv
 */

@ExperimentalApi(issue = "https://github.com/ydb-platform/yoj-project/pull/57")
@SupportedAnnotationTypes({
        "tech.ydb.yoj.databind.schema.Table",
})
@SupportedSourceVersion(SourceVersion.RELEASE_17)
@AutoService(Processor.class)
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
