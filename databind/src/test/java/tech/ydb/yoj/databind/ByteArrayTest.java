package tech.ydb.yoj.databind;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.TreeSet;

public class ByteArrayTest {
    @Test
    public void testWrap() {
        byte[] arr = bytesOf(0, 1, 2);
        var b = ByteArray.wrap(arr);
        b.getArray()[1] = 3;

        Assert.assertEquals(arr[1], 3);
    }

    @Test
    public void testCopy() {
        byte[] arr = bytesOf(0, 1, 2);
        var b = ByteArray.copy(arr);
        b.getArray()[1] = 3;

        Assert.assertEquals(arr[1], 1);
    }

    @Test
    public void testEquals() {
        byte[] arrA = bytesOf(0, 1, 2);
        byte[] arrB = bytesOf(0, 1, 2);
        var a = ByteArray.wrap(arrA);
        var b = ByteArray.wrap(arrB);

        Assert.assertNotEquals(arrA, arrB);
        Assert.assertEquals(a, a);
        Assert.assertEquals(a, b);
        Assert.assertNotEquals(a, null);
    }

    @Test
    public void testSetWork() {
        TreeSet<ByteArray> set = new TreeSet<>(List.of(
                valueOf(0, 1, 2),
                valueOf(0, 1, 2),
                valueOf(),
                valueOf(255),
                valueOf(1, 2, 3),
                valueOf(1, 2, 3),
                valueOf(0, 1, 3)
        ));

        Assert.assertEquals(set.stream().toList(), List.of(
                valueOf(),
                valueOf(255),
                valueOf(0, 1, 2),
                valueOf(0, 1, 3),
                valueOf(1, 2, 3)
        ));
    }

    @Test
    public void testCompare() {
        Assert.assertEquals(0, valueOf(0, 1, 2).compareTo(valueOf(0, 1, 2)));
        Assert.assertEquals(1, valueOf(0, 1, 3).compareTo(valueOf(0, 1, 2)));
        Assert.assertTrue(0 > valueOf(0, 1, 2).compareTo(valueOf(1, 2, 3)));
        Assert.assertTrue(0 > valueOf().compareTo(valueOf(1, 2, 3)));
        Assert.assertTrue(0 < valueOf(1, 2, 3).compareTo(valueOf()));
        Assert.assertTrue(0 < valueOf(0, 1, 2).compareTo(valueOf(0, 1)));
    }

    @Test
    public void testToString() {
        Assert.assertEquals("bytes()", valueOf().toString());
        Assert.assertEquals("bytes(00)", valueOf(0).toString());
        Assert.assertEquals("bytes(0102ff)", valueOf(1, 2, 255).toString());
        Assert.assertEquals("bytes(00a2fe00)", valueOf(0, 162, 254, 0).toString());
        Assert.assertEquals(
                "bytes(01010101010101010101010101010101)",
                valueOf(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1).toString()
        );
        Assert.assertEquals(
                "bytes(length > 16)",
                valueOf(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1).toString()
        );
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