package tech.ydb.yoj.databind;

import org.junit.Test;

import java.util.List;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

public class ByteArrayTest {
    @Test
    public void wrap() {
        byte[] arr = bytesOf(0, 1, 2);
        var b = ByteArray.wrap(arr);
        b.getArray()[1] = 3;

        assertThat(3).isEqualTo(arr[1]);
    }

    @Test
    public void copy() {
        byte[] arr = bytesOf(0, 1, 2);
        var b = ByteArray.copy(arr);
        b.getArray()[1] = 3;

        assertThat(1).isEqualTo(arr[1]);
    }

    @Test
    public void equality() {
        byte[] arrA = bytesOf(0, 1, 2);
        byte[] arrB = bytesOf(0, 1, 2);
        byte[] arrC = bytesOf(0, 1, 3);
        var a = ByteArray.wrap(arrA);
        var b = ByteArray.wrap(arrB);
        var c = ByteArray.wrap(arrC);

        assertThat(a).isEqualTo(a);
        assertThat(b).isEqualTo(b);
        assertThat(c).isEqualTo(c);

        assertThat(a).isEqualTo(b);
        assertThat(b).isEqualTo(a);

        assertThat(a).isNotEqualTo(null);
        assertThat(b).isNotEqualTo(null);

        assertThat(a).isNotEqualTo(c);
        assertThat(b).isNotEqualTo(c);
    }

    @Test
    public void sortedSet() {
        var set = new TreeSet<>(List.of(
                valueOf(0, 1, 2),
                valueOf(0, 1, 2),
                valueOf(),
                valueOf(255),
                valueOf(1, 2, 3),
                valueOf(1, 2, 3),
                valueOf(0, 1, 3)
        ));

        assertThat(set).containsExactly(
                valueOf(),
                valueOf(255),
                valueOf(0, 1, 2),
                valueOf(0, 1, 3),
                valueOf(1, 2, 3)
        );
    }

    @Test
    public void naturalOrdering() {
        assertThat(valueOf(0, 1, 2)).isEqualByComparingTo(valueOf(0, 1, 2));
        assertThat(valueOf(0, 1, 2)).isLessThan(valueOf(1, 2, 3));
        assertThat(valueOf(0, 1, 3)).isGreaterThan(valueOf(0, 1, 2));

        assertThat(valueOf()).isLessThan(valueOf(1, 2, 3));
        assertThat(valueOf(1, 2, 3)).isGreaterThan(valueOf());
        assertThat(valueOf(0, 1)).isLessThan(valueOf(0, 1, 2));
        assertThat(valueOf(0, 1, 2)).isGreaterThan(valueOf(0, 1));
    }

    @Test
    public void stringRepresentation() {
        assertThat(valueOf()).hasToString("bytes()");
        assertThat(valueOf(0)).hasToString("bytes(00)");
        assertThat(valueOf(1, 2, 255)).hasToString("bytes(0102ff)");
        assertThat(valueOf(0, 162, 254, 0)).hasToString("bytes(00a2fe00)");
        assertThat(valueOf(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1))
                .hasToString("bytes(01010101010101010101010101010101)");
        assertThat(valueOf(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)).hasToString("bytes(length > 16)");
    }

    private static byte[] bytesOf(int... array) {
        byte[] result = new byte[array.length];
        for (int i = 0; i < array.length; i++) {
            result[i] = (byte) array[i];
        }
        return result;
    }

    private static ByteArray valueOf(int... array) {
        byte[] result = new byte[array.length];
        for (int i = 0; i < array.length; i++) {
            result[i] = (byte) array[i];
        }
        return ByteArray.wrap(result);
    }
}
