package tech.ydb.yoj;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.CONSTRUCTOR;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.SOURCE;

/**
 * Annotates <em>experimental features</em>. These features are not part of the stable YOJ API: they can change incompatibly,
 * or even disappear entirely <em>in any release</em>.
 */
@Target({TYPE, FIELD, METHOD, PARAMETER, CONSTRUCTOR, ANNOTATION_TYPE})
@Retention(SOURCE)
public @interface ExperimentalApi {
    /**
     * @return URL of the GitHub issue tracking the experimental API
     */
    String issue();
}
