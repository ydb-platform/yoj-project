package tech.ydb.yoj.repository.test.inmemory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*package*/ final class InMemoryEntityLine {
    private final List<Versioned> versions;
    private final Map<Long, Columns> uncommited = new HashMap<>();

    private InMemoryEntityLine(List<Versioned> versions) {
        this.versions = versions;
    }

    public InMemoryEntityLine() {
        this(new ArrayList<>());
    }

    public InMemoryEntityLine createSnapshot() {
        return new InMemoryEntityLine(new ArrayList<>(versions));
    }

    public void commit(long txId, long version) {
        Columns uncommitedColumns = uncommited.remove(txId);
        if (uncommitedColumns == null) {
            return;
        }

        versions.add(new Versioned(version, uncommitedColumns));
    }

    public void rollback(long txId) {
        uncommited.remove(txId);
    }

    @Nullable
    public Columns get(long txId, long version) {
        Columns columns = getColumns(txId, version);
        return columns != Columns.EMPTY ? columns : null;
    }

    public void put(long txId, Columns columns) {
        uncommited.put(txId, columns);
    }

    public void remove(long txId) {
        uncommited.put(txId, Columns.EMPTY);
    }

    private Columns getColumns(long txId, long version) {
        Columns uncommitedColumns = uncommited.get(txId);
        if (uncommitedColumns != null) {
            return uncommitedColumns;
        }

        for (int i = versions.size() - 1; i >= 0; i--) {
            Versioned versioned = versions.get(i);
            if (versioned.version() <= version) {
                return versioned.columns();
            }
        }
        return Columns.EMPTY;
    }

    public boolean hasYounger(long version) {
        if (versions.isEmpty()) {
            return false;
        }
        return version < versions.get(versions.size() - 1).version();
    }

    private record Versioned(
            long version,
            Columns columns
    ) {
    }
}
