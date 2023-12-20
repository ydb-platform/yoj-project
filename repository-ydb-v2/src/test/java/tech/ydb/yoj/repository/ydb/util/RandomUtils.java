package tech.ydb.yoj.repository.ydb.util;

import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;

public class RandomUtils {
    private static final String alphanum = createAlphabet();

    private static String createAlphabet() {
        String upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        return upper + upper.toLowerCase(Locale.ROOT) + "0123456789";
    }

    public static String nextString(int length) {
        char[] buf = new char[length];
        for (int idx = 0; idx < buf.length; ++idx) {
            buf[idx] = alphanum.charAt(ThreadLocalRandom.current().nextInt(alphanum.length()));
        }
        return new String(buf);
    }
}
