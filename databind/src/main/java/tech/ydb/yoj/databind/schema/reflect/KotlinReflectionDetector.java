package tech.ydb.yoj.databind.schema.reflect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class KotlinReflectionDetector {
    private static final Logger log = LoggerFactory.getLogger(KotlinReflectionDetector.class);

    private KotlinReflectionDetector() {
    }

    public static final boolean kotlinAvailable = detectKotlinReflection();

    private static boolean detectKotlinReflection() {
        var cl = classLoader();

        try {
            Class.forName("kotlin.Metadata", false, cl);
        } catch (ClassNotFoundException e) {
            return false;
        }

        try {
            Class.forName("kotlin.reflect.full.KClasses", false, cl);
            return true;
        } catch (ClassNotFoundException e) {
            log.warn("YOJ has detected Kotlin but not kotlin-reflect. Kotlin data classes won't work as Entities.", e);
            return false;
        }
    }

    private static ClassLoader classLoader() {
        ClassLoader cl = null;
        try {
            cl = Thread.currentThread().getContextClassLoader();
        } catch (Exception ignore) {
        }
        if (cl == null) {
            cl = KotlinDataClassType.class.getClassLoader();
            if (cl == null) {
                try {
                    cl = ClassLoader.getSystemClassLoader();
                } catch (Exception ignore) {
                }
            }
        }
        return cl;
    }
}
