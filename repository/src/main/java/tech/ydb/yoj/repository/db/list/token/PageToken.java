package tech.ydb.yoj.repository.db.list.token;

import lombok.NonNull;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.list.BadListingException.InvalidPageToken;
import tech.ydb.yoj.repository.db.list.GenericListResult;
import tech.ydb.yoj.repository.db.list.ListRequest;
import tech.ydb.yoj.repository.db.list.ListResult;

import javax.annotation.Nullable;

/**
 * Encodes {@link ListResult search result pages} into reasonably compact <em>page tokens</em>,
 * and decodes them back into {@link ListRequest listing requests}.
 */
public interface PageToken {
    /**
     * "Empty" page token that allows to have only one page of results.
     * Its {@link #encode(GenericListResult) encode()} always returns {@code null},
     * and {@link #decode(ListRequest.Builder, String) decode()} always fails.
     */
    PageToken EMPTY = EmptyPageToken.INSTANCE;

    /**
     * Encodes information about next search result page.
     *
     * @param result current search result page
     * @param <T>    search request type
     * @param <R>    search result type
     * @return next page token or {@code null} if this is the last page of results
     */
    @Nullable
    <T extends Entity<T>, R> String encode(@NonNull GenericListResult<T, R> result);

    /**
     * Decodes page token into listing request.<br>
     * This method must be called <strong>only once</strong> per page, because it changes the listing request builder's
     * state and is therefore <strong>non-idempotent</strong>.
     *
     * @param bldr  listing request builder
     * @param token page token
     * @param <T>   search request type
     * @return listing request builder for to the page encoded by the token
     * @throws InvalidPageToken page token is invalid
     */
    @NonNull <T extends Entity<T>> ListRequest.Builder<T> decode(
            @NonNull ListRequest.Builder<T> bldr,
            @NonNull String token
    ) throws InvalidPageToken;
}
