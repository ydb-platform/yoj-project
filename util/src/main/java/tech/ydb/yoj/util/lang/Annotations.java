package tech.ydb.yoj.util.lang;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.util.Collection;
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
        A found = component.getAnnotation(annotation);
        if (found != null) {
            return found;
        }
        Set<Annotation> ann;
        if (component instanceof Class<?> clazz) {
            ann = collectAnnotations(clazz);
        } else {
            ann = Set.of(component.getAnnotations());
        }
        return findInDepth(annotation, ann);
    }

    @Nonnull
    private static Set<Annotation> collectAnnotations(Class<?> component) {
        Set<Annotation> result = new HashSet<>();
        Set<Class<?>> classesToExamine = new HashSet<>();
        classesToExamine.add(component);
        while (!classesToExamine.isEmpty()) {
            Class<?> candidate = classesToExamine.iterator().next();
            result.addAll(List.of(candidate.getDeclaredAnnotations()));
            if (candidate.getSuperclass() != null) {
                classesToExamine.add(candidate.getSuperclass());
            }
            classesToExamine.addAll(List.of(candidate.getInterfaces()));
            classesToExamine.remove(candidate);
        }
        return result;
    }

    @Nullable
    @SuppressWarnings("unchecked")
    private static <A extends Annotation> A findInDepth(Class<A> annotation, @Nonnull Collection<Annotation> anns) {
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
