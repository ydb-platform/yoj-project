package tech.ydb.yoj.util.lang;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class StringsTest {
    @Test
    public void join() {
        assertThat(Strings.join("?", "url/path", null)).isEqualTo("url/path");
        assertThat(Strings.join("?", "url/path", "")).isEqualTo("url/path");
        assertThat(Strings.join("?", "url/path", "a=1&b=2")).isEqualTo("url/path?a=1&b=2");
    }

    @Test
    public void testRemoveSuffix() {
        assertThat(Strings.removeSuffix("abc", "")).isEqualTo("abc");
        assertThat(Strings.removeSuffix("abc", "de")).isEqualTo("abc");
        assertThat(Strings.removeSuffix("abc", "bc")).isEqualTo("a");
        assertThat(Strings.removeSuffix("abc", "abc")).isEmpty();
        assertThat(Strings.removeSuffix("HelloWorldException", "Exception")).isEqualTo("HelloWorld");
        assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> Strings.removeSuffix("abc", null));
    }

    @Test
    public void testLeftPad() {
        assertThat(Strings.leftPad("3", 3, '0')).isEqualTo("003");
        assertThat(Strings.leftPad("003", 3, '0')).isEqualTo("003");
        assertThat(Strings.leftPad("0003", 3, '0')).isEqualTo("0003");
        assertThat(Strings.leftPad("  45", 6, ' ')).isEqualTo("    45");
    }
}
