package tech.ydb.yoj.databind.schema.reflect;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Annotations {
    private Annotations() {}

    public static <A extends Annotation> A find(Class<A> annotation, @NotNull AnnotatedElement component) {
        A column = component.getAnnotation(annotation);
        if (column != null) {
            return column;
        }
        return findInDepth(annotation, List.of(component.getAnnotations()));
    }

    @Nullable
    @SuppressWarnings("unchecked")
    private static <A extends Annotation> A findInDepth(Class<A> annotation, @NotNull List<Annotation> anns) {
        Set<Annotation> visited = new HashSet<>();
        Set<Annotation> annotationToExamine = new HashSet<>(anns);
        while (!annotationToExamine.isEmpty()) {
            Annotation candidate = annotationToExamine.iterator().next();
            if (visited.add(candidate)) {
                if (candidate.annotationType() == annotation) {
                    return (A) candidate;
                } else {
                    annotationToExamine.addAll(List.of(candidate.annotationType().getDeclaredAnnotations()));
                }
            }
            annotationToExamine.remove(candidate);
        }
        return null;
    }
}
