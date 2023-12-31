package tech.ydb.yoj.repository.ydb.client;

import com.yandex.ydb.table.settings.AutoPartitioningPolicy;
import com.yandex.ydb.table.settings.PartitioningPolicy;
import com.yandex.ydb.table.settings.PartitioningSettings;
import com.yandex.ydb.table.values.OptionalValue;
import com.yandex.ydb.table.values.PrimitiveValue;
import com.yandex.ydb.table.values.TupleValue;
import lombok.Value;

import java.util.stream.LongStream;

import static java.util.stream.Collectors.toList;

@Value
public class YdbTableHint {
    PartitioningPolicy partitioningPolicy;
    PartitioningSettings partitioningSettings;
    TablePreset tablePreset;

    public static YdbTableHint int64Range(int start, int end) {
        return new YdbTableHint(
                new PartitioningPolicy()
                        .setAutoPartitioning(AutoPartitioningPolicy.DISABLED)
                        .setExplicitPartitioningPoints(
                                LongStream.range(start, end)
                                        .mapToObj(part -> TupleValue.of(OptionalValue.of(PrimitiveValue.int64(part))))
                                .collect(toList())
                        ),
                null,
                null
        );
    }

    public static YdbTableHint uniform(int partitions) {
        return new YdbTableHint(
                new PartitioningPolicy()
                .setUniformPartitions(partitions),
                null,
                null
        );
    }

    public static YdbTableHint autoSplitByLoad(int exactPartitionsCount) {
        return autoSplitByLoad(exactPartitionsCount, exactPartitionsCount);
    }

    public static YdbTableHint autoSplitByLoad(int minPartitionsCount, int maxPartitionsCount) {
        return new YdbTableHint(
                new PartitioningPolicy()
                        .setAutoPartitioning(AutoPartitioningPolicy.AUTO_SPLIT_MERGE),
                new PartitioningSettings()
                        .setPartitioningByLoad(true)
                        .setMinPartitionsCount(minPartitionsCount)
                        .setMaxPartitionsCount(maxPartitionsCount),
                null
        );
    }

    public static YdbTableHint tablePreset(TablePreset tablePreset) {
        return new YdbTableHint(null, null, tablePreset);
    }

    public enum TablePreset {
        DEFAULT("default"),
        LOG_LZ4("log_lz4");

        private String tablePresetName;

        TablePreset(String tablePresetName) {
            this.tablePresetName = tablePresetName;
        }

        public String getTablePresetName() {
            return tablePresetName;
        }
    }
}
