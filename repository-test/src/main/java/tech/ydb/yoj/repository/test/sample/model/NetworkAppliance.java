package tech.ydb.yoj.repository.test.sample.model;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.Value;
import tech.ydb.yoj.databind.ByteArray;
import tech.ydb.yoj.databind.CustomValueType;
import tech.ydb.yoj.databind.converter.ValueConverter;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.Schema.JavaField;
import tech.ydb.yoj.repository.db.RecordEntity;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static tech.ydb.yoj.databind.DbType.UINT64;

public record NetworkAppliance(
        @NonNull Id id,
        @NonNull Ipv6Address address,
        @NonNull @Column(dbType = UINT64) NetworkAppliance.SixtyFourBitString sixtyFourBits
) implements RecordEntity<NetworkAppliance> {
    public record Id(@NonNull String value) implements RecordEntity.Id<NetworkAppliance> {
    }

    @Value
    @RequiredArgsConstructor
    @CustomValueType(columnClass = ByteArray.class, converter = Ipv6Address.Converter.class)
    public static class Ipv6Address {
        Inet6Address addr;

        public Ipv6Address(String ip6) throws UnknownHostException {
            this((Inet6Address) InetAddress.getByName(ip6));
        }

        @Override
        public String toString() {
            return addr.toString();
        }

        public static final class Converter implements ValueConverter<Ipv6Address, ByteArray> {
            @Override
            public @NonNull ByteArray toColumn(@NonNull JavaField field, @NonNull Ipv6Address ipv6Address) {
                return ByteArray.wrap(ipv6Address.addr.getAddress());
            }

            @NonNull
            @Override
            public Ipv6Address toJava(@NonNull JavaField field, @NonNull ByteArray bytes) {
                try {
                    return new Ipv6Address((Inet6Address) InetAddress.getByAddress(bytes.getArray()));
                } catch (UnknownHostException neverHappens) {
                    throw new InternalError(neverHappens);
                }
            }
        }
    }

    @CustomValueType(columnClass = Long.class, converter = LikeLong.Converter.class)
    public static abstract class LikeLong extends Number implements Comparable<LikeLong> {
        @Override
        public int intValue() {
            return (int) longValue();
        }

        @Override
        public double doubleValue() {
            return (double) longValue();
        }

        @Override
        public float floatValue() {
            return (float) longValue();
        }

        @Override
        public int compareTo(LikeLong o) {
            return Long.compare(this.longValue(), o.longValue());
        }

        public static final class Converter<L extends LikeLong> implements ValueConverter<L, Long> {
            @Override
            public @NonNull Long toColumn(@NonNull JavaField field, @NonNull L ll) {
                return ll.longValue();
            }

            @NonNull
            @Override
            @SneakyThrows
            @SuppressWarnings("unchecked")
            public L toJava(@NonNull JavaField field, @NonNull Long l) {
                return (L) field.getRawType().getConstructor(Long.class).newInstance(l);
            }
        }
    }

    @Value
    public static class SixtyFourBitString extends LikeLong {
        String hexString;

        // This constructor will be called if YOJ does not consider SixtyFourBitString to be a custom-value type, even if it is one.
        // This constructor will always fail with AssertionError.
        @SuppressWarnings("unused")
        private SixtyFourBitString(@NonNull String ignore) {
            throw new AssertionError("SixtyFourBitString(String) must not be called while constructing SixtyFourBitString");
        }

        // Should be used reflectively by LikeLong.Converter
        @SuppressWarnings("unused")
        public SixtyFourBitString(@NonNull Long longValue) {
            this.hexString = Long.toUnsignedString(longValue, 16);
        }

        // Should be used by LikeLong.Converter
        @Override
        public long longValue() {
            return Long.parseUnsignedLong(hexString, 16);
        }

        @Override
        public String toString() {
            return hexString;
        }
    }
}
