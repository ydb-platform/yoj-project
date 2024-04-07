package tech.ydb.yoj.generator;

import com.google.common.base.CaseFormat;
import com.google.common.base.Strings;

import javax.annotation.Nullable;

public class StringConstantsRenderer {

    /**
     * returns the source code of the class
     */
    public static String render(TargetClassStructure targetClass, @Nullable String packageName) {

        String ident = Strings.repeat(" ", targetClass.nestLevel() * 4);
        StringBuilder result = new StringBuilder();

        // header
        if (packageName != null) {
            result.append("""
            package %s;
       
            import javax.annotation.processing.Generated;
            
            @Generated("%s")
            public final class %s {
            """.formatted(
                            packageName,
                            FieldGeneratorAnnotationProcessor.class.getName(),
                            targetClass.className()
                    )
            );
        } else {
            // Nested class
            result.append(ident)
                    .append("public static final class %s {\n".formatted(targetClass.className()));
        }

        // private c-tor, to make it non-instantiable
        result.append(ident)
                .append("    private %s(){}\n\n".formatted(targetClass.className()));

        // fields
        targetClass.simpleFieldNames().stream()
                .map(name -> renderField(targetClass.fieldPrefix(), name))
                .map(s -> ident + "    " + s + "\n")
                .forEach(result::append);

        // nested classes
        for (TargetClassStructure nestedClass : targetClass.nestedClasses()) {
            result.append(render(nestedClass, null));
        }

        // Close
        result.append(ident).append("}\n");

        return result.toString();
    }

    private static String renderField(String fieldPrefix, String simpleFieldName) {
        return "public static final String %s = \"%s\";".formatted(
                CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, simpleFieldName),
                Utils.concatFieldNameChain(fieldPrefix, simpleFieldName)
        );
    }

}
