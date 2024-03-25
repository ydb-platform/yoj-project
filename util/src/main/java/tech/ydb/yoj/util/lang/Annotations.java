package tech.ydb.yoj.util.lang;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class Annotations {
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
        return found != null ? found : findInDepth(annotation, component);
    }

    @Nonnull
    private static Set<Annotation> collectAnnotations(Class<?> component) {
        Set<Annotation> result = new HashSet<>();
        Set<Class<?>> classesToExamine = new HashSet<>();
        classesToExamine.add(component);
        while (!classesToExamine.isEmpty()) {
            Class<?> candidate = classesToExamine.iterator().next();
            nonJdkAnnotations(candidate.getDeclaredAnnotations())
                    .forEach(result::add);
            if (candidate.getSuperclass() != null && !jdkClass(candidate.getSuperclass())) {
                classesToExamine.add(candidate.getSuperclass());
            }
            for (Class<?> in : candidate.getInterfaces()) {
                if (!jdkClass(in)) {
                    classesToExamine.add(in);
                }
            }
            classesToExamine.remove(candidate);
        }
        return result;
    }

    @Nullable
    @SuppressWarnings("unchecked")
    private static <A extends Annotation> A findInDepth(Class<A> annotation, @Nonnull AnnotatedElement component) {
        Set<Annotation> visited = new HashSet<>();
        Set<Annotation> annotationToExamine = getAnnotations(component);
        while (!annotationToExamine.isEmpty()) {
            Annotation candidate = annotationToExamine.iterator().next();
            if (visited.add(candidate)) {
                if (candidate.annotationType() == annotation) {
                    return (A) candidate;
                } else {
                    nonJdkAnnotations(candidate.annotationType().getDeclaredAnnotations())
                            .forEach(annotationToExamine::add);
                }
            }
            annotationToExamine.remove(candidate);
        }
        return null;
    }

    private static Set<Annotation> getAnnotations(AnnotatedElement component) {
        if (component instanceof Class<?> clazz) {
            return jdkClass(clazz) ? new HashSet<>() : collectAnnotations(clazz);
        } else {
            return nonJdkAnnotations(component.getAnnotations()).collect(Collectors.toSet());
        }
    }

    private static Stream<Annotation> nonJdkAnnotations(Annotation[] annotations) {
        return Arrays.stream(annotations)
                .filter(da -> !jdkClass(da.annotationType()));
    }

    static boolean jdkClass(Class<?> type) {
        return type.getName().startsWith("java.");
    }
}
