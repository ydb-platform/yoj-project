package tech.ydb.yoj;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@InternalApi
public final class DeprecationWarnings {
    private static final Logger log = LoggerFactory.getLogger(DeprecationWarnings.class);
    private static final Set<String> warnings = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private DeprecationWarnings() {
    }

    public static void warnOnce(String key, String msg, Object... args) {
        if (warnings.add(key)) {
            var formattedMsg = Strings.lenientFormat(msg, args);
            log.warn(formattedMsg, new Throwable(key + " stack trace"));
        }
    }
}
