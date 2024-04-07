package tech.ydb.yoj.generator;

import com.google.common.base.CaseFormat;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A recursive structure describing a resulting class which will be generated by the annotation processor
 *
 * @param className name of the target class
 * @param simpleFieldNames names of fields considered as 'simple'
 * @param nestedClasses list of nested classes which describes non-simple fields
 * @param fieldPrefix Let's consider a following source class example:
 *                         <pre>{@code
 *                                                 class RootClass {
 *                                                     NestedClass parent;
 *                                                     class NestedClass {
 *                                                         String child;
 *                                                     }
 *                                                  }
 *                                                 }
 *                                                 </pre>
 *                         <p>Then RootClassFields and nested RootClassFields.Parent will be generated.</p>
 *                         RootClassFields's fieldPrefix will be '' and RootClassFields.Parent's fieldPrefix is 'parent'
 * @param originatingField the name of the field which was the cause of this nested class. Null for root class
 * @param nestLevel nesting level. 0 is a root class.
 */
record TargetClassStructure(
        String className,
        List<String> simpleFieldNames,
        List<TargetClassStructure> nestedClasses,
        String fieldPrefix,
        @Nullable String originatingField,
        int nestLevel
) {

    public static TargetClassStructure build(SourceClassStructure sourceClassStructure, String targetClassName) {
        return new Builder(sourceClassStructure).build(
                sourceClassStructure,
                targetClassName,
                //  it's a root so:
                "", // No prefix
                null, // No originating field
                0 // Top level
        );
    }

    private static class Builder {
        private final Map<String, SourceClassStructure> allAvailableNestedClasses = new HashMap<>();

        public Builder(SourceClassStructure root) {
            calcAllAvailableNestedClasses(root, allAvailableNestedClasses);
        }

        public TargetClassStructure build(
                SourceClassStructure sourceClassStructure,
                String className,
                String fieldPrefix,
                @Nullable String originatingField,
                int nestLevel
        ) {
            List<String> fields = new ArrayList<>();
            List<TargetClassStructure> nestedClasses = new ArrayList<>();

            for (FieldInfo field : sourceClassStructure.fields()) {
                int amountOfFieldsInside = calcAmountOfFieldsInside(field);

                if (isSimpleField(field)) {
                    fields.add(field.name());

                } else if (isIdField(field) && amountOfFieldsInside == 1) {
                    // Special case for Id with one field
                    fields.add(field.name());

                } else if (amountOfFieldsInside > 0) { // Non-empty complex field
                    nestedClasses.add(
                            build(
                                    getComplexFieldClass(field),
                                    // className is a field's name in UpperCamel case
                                    CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, field.name()),
                                    // fieldPrefix
                                    Utils.concatFieldNameChain(fieldPrefix, field.name()),
                                    field.name(),
                                    nestLevel + 1
                            )
                    );
                }
            }

            return new TargetClassStructure(className, fields, nestedClasses, fieldPrefix, originatingField, nestLevel);
        }

        public boolean isSimpleField(FieldInfo field) {
            return !allAvailableNestedClasses.containsKey(field.type());
        }

        @Nonnull
        public SourceClassStructure getComplexFieldClass(FieldInfo field) {
            SourceClassStructure structure = allAvailableNestedClasses.get(field.type());
            if (structure == null) {
                throw new IllegalArgumentException(
                        "Field %s of type %s is not considered to be complex!".formatted(
                                field.name(),
                                field.type()
                        )
                );
            }
            return structure;
        }

        /**
         * Calculate amount of fields inside the given one. E.g. if the field is simple return 1, if field is complex
         * it will go inside of it and calculate how much fields inside the nested class
         */
        public int calcAmountOfFieldsInside(FieldInfo field) {
            if (isSimpleField(field)) {
                return 1;
            }
            int fieldsInside = 0;
            for (FieldInfo innerField : getComplexFieldClass(field).fields()) {
                fieldsInside += calcAmountOfFieldsInside(innerField);
            }
            return fieldsInside;
        }

        private static boolean isIdField(FieldInfo field) {
            // The better way would be to check that the fields' type implements Entity.Id class.
            // However, since the Entity interface is publicly available,
            // I don't think that renaming of the field will happen
            return field.name().equals("id");
        }

        private static void calcAllAvailableNestedClasses(
                SourceClassStructure root,
                Map<String, SourceClassStructure> result
        ) {
            result.putAll(root.nestedClasses());
            root.nestedClasses().values().forEach(nested -> calcAllAvailableNestedClasses(nested, result));
        }
    }
}
