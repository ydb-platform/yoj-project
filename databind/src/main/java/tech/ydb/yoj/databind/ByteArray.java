package tech.ydb.yoj.databind;

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

public final class ByteArray implements Comparable<ByteArray> {
    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

    private final byte[] array;

    private ByteArray(byte[] array) {
        this.array = array;
    }

    public static ByteArray wrap(byte[] array) {
        return new ByteArray(array);
    }

    public static ByteArray copy(byte[] array) {
        return new ByteArray(array.clone());
    }

    public ByteArray copy() {
        return ByteArray.copy(array);
    }

    public byte[] getArray() {
        return array;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ByteArray that = (ByteArray) o;
        return Arrays.equals(array, that.array);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(array);
    }

    @Override
    public int compareTo(@NotNull ByteArray o) {
        return Arrays.compare(array, o.array);
    }

    @Override
    public String toString() {
        if (array.length > 16) {
            return "bytes(length > 16)";
        }

        char[] hexChars = new char[array.length * 2 + 7];
        hexChars[0] = 'b';
        hexChars[1] = 'y';
        hexChars[2] = 't';
        hexChars[3] = 'e';
        hexChars[4] = 's';
        hexChars[5] = '(';

        int i = 0;
        for (; i < array.length; i++) {
            int v = array[i] & 0xFF;
            int ci = i * 2 + 6;
            hexChars[ci] = HEX_CHARS[v >>> 4];
            hexChars[ci + 1] = HEX_CHARS[v & 0x0F];
        }
        hexChars[i * 2 + 6] = ')';

        return new String(hexChars);
    }
}
