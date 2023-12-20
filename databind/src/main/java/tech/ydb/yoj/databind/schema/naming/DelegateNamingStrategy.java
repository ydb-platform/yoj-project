package tech.ydb.yoj.databind.schema.naming;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.experimental.Delegate;

@AllArgsConstructor
@EqualsAndHashCode
public class DelegateNamingStrategy implements NamingStrategy {
    @Delegate
    protected final NamingStrategy delegate;
}
