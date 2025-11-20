package tech.ydb.yoj.repository.db.list.token;

import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.list.BadListingException.InvalidPageToken;
import tech.ydb.yoj.repository.db.list.GenericListResult;
import tech.ydb.yoj.repository.db.list.ListRequest;

import javax.annotation.Nullable;

import static lombok.AccessLevel.PRIVATE;

/**
 * Page token which eases migration from one token implementation (<em>fallback</em>) to another (<em>primary</em>).
 * It attempts to decode page token first using primary implementation, and, failing that, using the fallback;
 * and can encode listing results using either <em>fallback</em> or <em>primary</em>, depending on the value of
 * {@link FallbackPageTokenBuilder#encodeAsPrimary(boolean) encodeAsPrimary} flag.
 * <p>
 * Changing old token implementation {@code oldImpl} to new implementation {@code newImpl} should go as follows:
 * <ol>
 *     <li>Deploy the app using {@code FallbackPageToken(primary=newImpl, fallback=oldImpl, encodeAsPrimary=false)}
 * so that all app instances encode listing results using the least common denominator, {@code oldImpl}.</li>
 *     <li>Deploy the app using {@code FallbackPageToken(primary=newImpl, fallback=oldImpl, encodeAsPrimary=true)}.
 * Old tokens can still be decoded, but all app instances will encode their listing results using {@code newImpl}.</li>
 *     <li>Deploy new application version using {@code newImpl} only, because no app instances use {@code oldImpl}
 * to encode listing results any more.</li>
 * </ol>
 */
@Value
@Builder
@RequiredArgsConstructor(access = PRIVATE)
public class FallbackPageToken implements PageToken {
    @NonNull
    PageToken primary;

    @NonNull
    PageToken fallback;

    boolean encodeAsPrimary;

    @Nullable
    @Override
    public <T extends Entity<T>, R> String encode(@NonNull GenericListResult<T, R> result) {
        return (encodeAsPrimary ? primary : fallback).encode(result);
    }

    @NonNull
    @Override
    public <T extends Entity<T>> ListRequest.Builder<T> decode(
            @NonNull ListRequest.Builder<T> bldr,
            @NonNull String token
    ) throws InvalidPageToken {
        try {
            return primary.decode(bldr, token);
        } catch (InvalidPageToken primaryFailed) {
            try {
                return fallback.decode(bldr, token);
            } catch (InvalidPageToken fallbackFailed) {
                primaryFailed.addSuppressed(fallbackFailed);
                throw primaryFailed;
            }
        }
    }
}
