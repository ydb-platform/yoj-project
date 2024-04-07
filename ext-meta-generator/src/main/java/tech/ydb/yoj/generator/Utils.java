package tech.ydb.yoj.generator;

import com.google.common.base.Strings;

public final class Utils {
    private Utils() {
    }

    public static String concatFieldNameChain(String one, String two) {
        if (Strings.isNullOrEmpty(one)) {
            return two;
        }
        if (Strings.isNullOrEmpty(two)) {
            return one;
        }
        return one + "." + two;
    }
}
