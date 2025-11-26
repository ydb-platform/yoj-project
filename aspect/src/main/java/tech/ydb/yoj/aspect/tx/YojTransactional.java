package tech.ydb.yoj.aspect.tx;

import tech.ydb.yoj.repository.db.IsolationLevel;
import tech.ydb.yoj.repository.db.TxManager;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method or class as code that should be executed in a YOJ transaction.
 * <p>
 * Real transaction is managed by the aspect class, {@link YojTransactionAspect}, that you need to wire into your
 * configuration (most likely, using Spring AOP).
 *
 * @see YojTransactionAspect
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.ANNOTATION_TYPE})
public @interface YojTransactional {
    /**
     * Special value meaning "use the default retry count that is set on the {@code TxManager}".
     */
    int UNDEFINED = -1;

    /**
     * Marks this transaction as read-only.
     */
    boolean readOnly() default false;

    /**
     * Specifies isolation level for the transaction.
     *
     * <p>If transaction marked as {@link #readOnly() read-only}, then following isolation levels are supported:
     * <ul>
     * <li>{@code ONLINE_CONSISTENT_READ_ONLY}</li>
     * <li>{@code ONLINE_INCONSISTENT_READ_ONLY}</li>
     * <li>{@code STALE_CONSISTENT_READ_ONLY}</li>
     * <li>{@code SNAPSHOT}</li>
     * </ul>
     * If transaction is not marked as {@link #readOnly() read-only}, isolation level is ignored.
     */
    IsolationLevel isolation() default IsolationLevel.ONLINE_CONSISTENT_READ_ONLY;

    /**
     * Sets a custom transaction name. If empty, the method name will be used as transaction name.
     */
    String name() default "";

    /**
     * Specifies transaction propagation mode.
     *
     * <p>To better separate Service and Repository layers in a DDD architecture, you should only establish transactions
     * in the Service layer (with {@link Propagation#NEVER}), and not in the Repository layer, which will always use the
     * current established transaction (or fail, potentially highlighting calls to Repositories from outside Services).
     *
     * @see Propagation
     */
    Propagation propagation() default Propagation.NEVER;

    /**
     * Specifies custom transaction retry count for the annotated method.
     * <p>Must be either >= 0 or {@link #UNDEFINED} ("use the retry count that is set on the {@code TxManager}").
     */
    int maxRetries() default UNDEFINED;

    /**
     * Specifies exception's list that doesn't lead to transaction rollback
     */
    Class<? extends Throwable>[] noRollbackFor() default {};

    /**
     * Specifies how writes are performed in this transaction.
     * If transaction is {@link #readOnly() read-only}, the write mode is ignored.
     *
     * @see WriteMode#DELAYED
     * @see WriteMode#IMMEDIATE
     */
    WriteMode writeMode() default WriteMode.UNSPECIFIED;

    /**
     * Transaction propagation mode.
     */
    enum Propagation {
        /**
         * Creates new transaction, throws exception if transaction already exists.
         */
        NEVER,
        /**
         * Uses current transaction if it exists; creates a new transaction otherwise.
         */
        REQUIRED,
        /**
         * Always creates a new transaction.
         * <p><strong>Note</strong>: YDB doesn't support nested transactions, so this new transaction might commit while
         * the previous transaction might roll back, or it might see entity state different from that of the previous
         * transaction, potentially creating data inconsistencies.
         * <br>This option is useful if you need to do a massive read-only transaction or scan, with short batches
         * of write transactions operating on the data just read (e.g., a data migration), and vice versa.
         * Just remember to re-read the data in a read-write transaction before usage, or do only idempotent actions,
         * like deleting entities with unique, never repeating IDs.
         */
        REQUIRES_NEW,
    }

    /**
     * Write mode for this read-write transaction.
     */
    enum WriteMode {
        /**
         * Uses the write mode specified on the {@code TxManager} that {@code YojTransactionAspect} uses.
         * This will typically (but <strong>not</strong> always!) be {@link #DELAYED delayed writes}.
         */
        UNSPECIFIED,

        /**
         * Enables pending write queue in transaction and execute write changes right before the transaction
         * is committed.
         * <p>Note that in this mode, you can still see <em>some</em> of transaction's own changes <em>iif</em>
         * you query your entities by using {@code Table.find(ID)} or {@code Table.find(Set<ID>)}
         * (which also permits partial IDs for range queries) <strong>and</strong> you did not also disable
         * the first-level cache.
         *
         * @see TxManager#delayedWrites()
         */
        DELAYED,

        /**
         * Disables pending write queue in transaction and executes write changes immediately.
         *
         * <p><strong>Note:</strong> This also disables write merging, which <em>significantly</em> impacts write
         * performance, and will only work on YDB <a href="https://ydb.tech/docs/en/changelog-server#23-3">&ge; 23.3</a>
         * where transactions can see their own changes.
         * <br>Please enable this option <strong>only</strong> if your business logic requires writing changes and then
         * reading them from the same transaction via a non-trivial query (not a {@code Table.find(ID)} and not
         * a {@code Table.find(Set<ID>)}), e.g., via {@code Table.query()} DSL, or even {@code Table.find(Range<ID>)}.
         *
         * @see TxManager#immediateWrites()
         */
        IMMEDIATE,
    }
}
