package tech.ydb.yoj.generator;

import javax.annotation.Nonnull;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The structure of input class and its nested classes.
 * @param className
 * @param fields all non-static fields, doesn't matter whether they are 'simple' or not
 * @param nestedClasses map < fieldTypeName, SourceClassStructure >
 * @param nestLevel since the same structure is used for nested classes, this field indicates how
 *                  nested the class is. 0 means it's a root class.
 */
record SourceClassStructure(
        String className,
        List<FieldInfo> fields,
        Map<String, SourceClassStructure> nestedClasses,
        int nestLevel
) {
    public static SourceClassStructure analyse(Element root, Types typeUtils) {
        return analyse(root, 0, typeUtils);
    }

    /** Recursively analyse the given class and all its nested classes */
    private static SourceClassStructure analyse(Element classElement, int nestLevel, Types typeUtils) {

        final String className = ((TypeElement) classElement).getQualifiedName().toString();
        final List<FieldInfo> fields = FieldInfo.extractAllFields(classElement, typeUtils);
        final Map<String, SourceClassStructure> nestedClasses = analyseNestedClasses(classElement, nestLevel, typeUtils);

        return new SourceClassStructure(className, fields, nestedClasses, nestLevel);
    }

    private static Map<String, SourceClassStructure> analyseNestedClasses(Element root, int nestLevel, Types typeUtils) {
        return root.getEnclosedElements()
                .stream()
                .filter(e -> e.getKind() == ElementKind.CLASS || e.getKind() == ElementKind.RECORD)
                .map(e -> analyse(e, nestLevel + 1, typeUtils))
                .collect(Collectors.toMap(SourceClassStructure::className, Function.identity()));
    }
}
