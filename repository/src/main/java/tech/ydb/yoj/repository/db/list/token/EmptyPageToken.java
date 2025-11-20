package tech.ydb.yoj.repository.db.list.token;

import lombok.NonNull;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.list.BadListingException.InvalidPageToken;
import tech.ydb.yoj.repository.db.list.GenericListResult;
import tech.ydb.yoj.repository.db.list.ListRequest;

import javax.annotation.Nullable;

/**
 * The most trivial implementation of {@link PageToken}: it does not produce page tokens, and is thus only suitable when
 * the listing produces at most one page of results.
 */
public final class EmptyPageToken implements PageToken {
    public static final PageToken INSTANCE = new EmptyPageToken();

    @Nullable
    @Override
    public <T extends Entity<T>, R> String encode(@NonNull GenericListResult<T, R> result) {
        return null;
    }

    @NonNull
    @Override
    public <T extends Entity<T>> ListRequest.Builder<T> decode(
            @NonNull ListRequest.Builder<T> bldr,
            @NonNull String token
    ) throws InvalidPageToken {
        throw new InvalidPageToken();
    }
}
