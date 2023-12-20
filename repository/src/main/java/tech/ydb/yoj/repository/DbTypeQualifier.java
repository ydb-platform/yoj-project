package tech.ydb.yoj.repository;

import tech.ydb.yoj.repository.db.Repository;

/**
 * Common database type qualifiers that can be used with any {@link Repository Repository}
 * implementation.
 */
public interface DbTypeQualifier {
    /**
     * Timestamp is represented as epoch in seconds.
     */
    String SECONDS = "Seconds";

    /**
     * Timestamp is represented as epoch in milliseconds.
     */
    String MILLISECONDS = "Milliseconds";

    /**
     * Serialize enum using name()
     */
    String ENUM_NAME = "name";

    /**
     * Serialize enum using toString()
     */
    String ENUM_TO_STRING = "toString";
}
