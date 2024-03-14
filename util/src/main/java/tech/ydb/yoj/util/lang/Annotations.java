package tech.ydb.yoj.util.lang;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Annotations {
    private Annotations() {
    }

    /**
     * Find first annotation that matches the class in parameter
     *
     * @param annotation - to look for
     * @param component - entry point to search
     *
     * @return annotation found or null
     */

    @Nullable
    public static <A extends Annotation> A find(Class<A> annotation, @Nonnull AnnotatedElement component) {
        A column = component.getAnnotation(annotation);
        if (column != null) {
            return column;
        }
        return findInDepth(annotation, List.of(component.getAnnotations()));
    }

    @Nullable
    @SuppressWarnings("unchecked")
    private static <A extends Annotation> A findInDepth(Class<A> annotation, @Nonnull List<Annotation> anns) {
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
