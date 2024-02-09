package tech.ydb.yoj.repository.test.sample.model;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import tech.ydb.yoj.databind.CustomValueType;
import tech.ydb.yoj.databind.ValueConverter;
import tech.ydb.yoj.repository.db.RecordEntity;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static tech.ydb.yoj.databind.FieldValueType.BINARY;

public record NetworkAppliance(
        @NonNull Id id,
        @NonNull Ipv6Address address
) implements RecordEntity<NetworkAppliance> {
    public record Id(@NonNull String value) implements RecordEntity.Id<NetworkAppliance> {
    }

    @CustomValueType(columnValueType = BINARY, columnClass = byte[].class, converter = Ipv6Address.Converter.class)
    @Value
    @RequiredArgsConstructor
    public static class Ipv6Address {
        Inet6Address addr;

        public Ipv6Address(String ip6) throws UnknownHostException {
            this((Inet6Address) InetAddress.getByName(ip6));
        }

        public static final class Converter implements ValueConverter<Ipv6Address, byte[]> {
            @Override
            public byte @NonNull [] toColumn(@NonNull Ipv6Address ipv6Address) {
                return ipv6Address.addr.getAddress();
            }

            @NonNull
            @Override
            public Ipv6Address toJava(byte @NonNull [] bytes) {
                try {
                    return new Ipv6Address((Inet6Address) InetAddress.getByAddress(bytes));
                } catch (UnknownHostException neverHappens) {
                    throw new InternalError(neverHappens);
                }
            }
        }
    }
}
