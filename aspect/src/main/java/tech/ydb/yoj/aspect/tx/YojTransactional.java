package tech.ydb.yoj.aspect.tx;

import tech.ydb.yoj.repository.db.IsolationLevel;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks method or class as code that should be executed in transaction
 * <p>
 * Real transaction is managed by aspect class
 *
 * @see YojTransactionAspect
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.ANNOTATION_TYPE})
public @interface YojTransactional {
    int UNDEFINED = -1;
    /**
     * Mark transactions as read only
     */
    boolean readOnly() default false;

    /**
     * Indicates isolation level of transaction.
     * If transaction marked as read only, then following isolation levels are supported:
     * ONLINE_CONSISTENT_READ_ONLY
     * ONLINE_INCONSISTENT_READ_ONLY
     * STALE_CONSISTENT_READ_ONLY
     * SNAPSHOT
     * If transaction marked as not read only, isolation level is ignored.
     */
    IsolationLevel isolation() default IsolationLevel.ONLINE_CONSISTENT_READ_ONLY;

    /**
     * Set transaction name otherwise method name will be used as transaction name
     */
    String name() default "";

    /**
     * Here I'm trying to force separate Service and Repository layers
     */
    Propagation propagation() default Propagation.NEVER;

    /**
     * Specifies custom retries count for method annotated
     */
    int maxRetries() default UNDEFINED;

    enum Propagation {
        /**
         * Creates new transaction, throws exception if transaction already exists
         */
        NEVER,
        /**
         * Uses current transaction if exist or creates new otherwise
         */
        REQUIRED,
        /**
         * Always creates new transaction (Note, YDB doesn't support nested transactions)...
         */
        REQUIRES_NEW,
    }
}
