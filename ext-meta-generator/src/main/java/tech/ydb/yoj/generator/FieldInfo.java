package tech.ydb.yoj.generator;

import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.util.Types;
import java.util.List;

/**
 * Information about all fields in a source class
 * @param name the original name of a field
 * @param type the full type name of an original field
 */
record FieldInfo(String name, String type) {

    public static List<FieldInfo> extractAllFields(Element classElement, Types typeUtils) {
        return classElement.getEnclosedElements().stream()
                .filter(FieldInfo::isNonStaticField)
                .map(element -> FieldInfo.extractField(element, typeUtils))
                .toList();
    }

    private static FieldInfo extractField(Element fieldElementName, Types typeUtils) {
        return new FieldInfo(
                fieldElementName.getSimpleName().toString(),
                calcType(fieldElementName, typeUtils)
        );
    }

    /*
        The implementation looks like heresy, but it's necessary.
        If you just call `element.asType().toString()` on an element with an annotation,
        it will return a string with a type and the annotation's name; but we need ONLY the type
     */
    private static String calcType(Element element, Types typeUtils) {
        Element nonPrimitiveType = typeUtils.asElement(element.asType());

        if (nonPrimitiveType != null && (
                nonPrimitiveType.getKind() == ElementKind.CLASS ||
                nonPrimitiveType.getKind() == ElementKind.RECORD
        )) {
            return ((TypeElement) nonPrimitiveType).getQualifiedName().toString();
        } else {
            return "primitive";
        }
    }

    private static boolean isNonStaticField(Element e) {
        if (e.getKind() != ElementKind.FIELD) {
            return false;
        }
        VariableElement variableElement = (VariableElement) e;
        //noinspection RedundantIfStatement
        if (variableElement.getModifiers().contains(Modifier.STATIC)) {
            return false;
        }
        return true;
    }
}
