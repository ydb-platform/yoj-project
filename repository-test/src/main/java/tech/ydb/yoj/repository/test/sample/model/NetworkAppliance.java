package tech.ydb.yoj.repository.test.sample.model;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import tech.ydb.yoj.databind.ByteArray;
import tech.ydb.yoj.databind.CustomValueType;
import tech.ydb.yoj.databind.converter.ValueConverter;
import tech.ydb.yoj.databind.schema.Schema.JavaField;
import tech.ydb.yoj.repository.db.RecordEntity;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static tech.ydb.yoj.databind.FieldValueType.BYTE_ARRAY;

public record NetworkAppliance(
        @NonNull Id id,
        @NonNull Ipv6Address address
) implements RecordEntity<NetworkAppliance> {
    public record Id(@NonNull String value) implements RecordEntity.Id<NetworkAppliance> {
    }

    @CustomValueType(columnValueType = BYTE_ARRAY, columnClass = ByteArray.class, converter = Ipv6Address.Converter.class)
    @Value
    @RequiredArgsConstructor
    public static class Ipv6Address {
        Inet6Address addr;

        public Ipv6Address(String ip6) throws UnknownHostException {
            this((Inet6Address) InetAddress.getByName(ip6));
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
}
