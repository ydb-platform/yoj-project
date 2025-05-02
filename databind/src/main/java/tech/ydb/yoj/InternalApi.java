package tech.ydb.yoj;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.CONSTRUCTOR;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.MODULE;
import static java.lang.annotation.ElementType.PACKAGE;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.RECORD_COMPONENT;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.SOURCE;

/**
 * Annotates internal YOJ implementation details (classes, interfaces, methods, fields, constants, etc.) that need to be
 * {@code public} to be used from different and/or multiple packages, but are not stable even across minor YOJ releases.
 * <p>Non-{@code public} (e.g., package-private) classes, interfaces, methods, fields, constants etc. in YOJ are assumed
 * to be internal implementation details regardless of the presence of an {@code @InternalApi} annotation on them.
 */
@Target({TYPE, FIELD, METHOD, PARAMETER, CONSTRUCTOR, ANNOTATION_TYPE, PACKAGE, MODULE, RECORD_COMPONENT})
@Retention(SOURCE)
public @interface InternalApi {
}
