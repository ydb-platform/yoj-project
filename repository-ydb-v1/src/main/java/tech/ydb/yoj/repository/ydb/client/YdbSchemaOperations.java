package tech.ydb.yoj.repository.ydb.client;

import com.yandex.ydb.ValueProtos;
import com.yandex.ydb.core.Result;
import com.yandex.ydb.core.Status;
import com.yandex.ydb.core.grpc.GrpcTransport;
import com.yandex.ydb.scheme.SchemeOperationProtos;
import com.yandex.ydb.table.SchemeClient;
import com.yandex.ydb.table.Session;
import com.yandex.ydb.table.description.ListDirectoryResult;
import com.yandex.ydb.table.description.TableDescription;
import com.yandex.ydb.table.description.TableIndex;
import com.yandex.ydb.table.rpc.grpc.GrpcSchemeRpc;
import com.yandex.ydb.table.settings.AlterTableSettings;
import com.yandex.ydb.table.settings.Changefeed;
import com.yandex.ydb.table.settings.CreateTableSettings;
import com.yandex.ydb.table.settings.PartitioningPolicy;
import com.yandex.ydb.table.settings.PartitioningSettings;
import com.yandex.ydb.table.settings.TtlSettings;
import com.yandex.ydb.table.values.OptionalType;
import com.yandex.ydb.table.values.PrimitiveType;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.With;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.exception.CreateTableException;
import tech.ydb.yoj.repository.db.exception.DropTableException;
import tech.ydb.yoj.repository.ydb.exception.SnapshotCreateException;
import tech.ydb.yoj.repository.ydb.exception.YdbRepositoryException;
import tech.ydb.yoj.repository.ydb.exception.YdbSchemaPathNotFoundException;
import tech.ydb.yoj.repository.ydb.yql.YqlType;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.yandex.ydb.core.StatusCode.SCHEME_ERROR;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static lombok.AccessLevel.PRIVATE;
import static tech.ydb.yoj.repository.ydb.client.YdbIssue.DEFAULT_ERROR;
import static tech.ydb.yoj.repository.ydb.client.YdbPaths.canonicalRootDir;
import static tech.ydb.yoj.repository.ydb.client.YdbPaths.canonicalTablespace;
import static tech.ydb.yoj.repository.ydb.client.YdbPaths.tableDirectory;

@Getter
public class YdbSchemaOperations {
    private static final Logger log = LoggerFactory.getLogger(YdbSchemaOperations.class);

    private String tablespace;
    private final SessionManager sessionManager;
    private final SchemeClient schemeClient;

    public YdbSchemaOperations(String tablespace, @NonNull SessionManager sessionManager, GrpcTransport transport) {
        this.tablespace = canonicalTablespace(tablespace);
        this.sessionManager = sessionManager;
        this.schemeClient = SchemeClient.newClient(GrpcSchemeRpc.useTransport(transport)).build();
    }

    public void setTablespace(String tablespace) {
        this.tablespace = canonicalTablespace(tablespace);
    }

    public void createTablespace() {
        mkdirs(canonicalRootDir(tablespace));
    }

    public boolean tablespaceExists() {
        return hasPath(canonicalRootDir(tablespace));
    }

    @SneakyThrows
    public void createTable(String name, List<EntitySchema.JavaField> columns, List<EntitySchema.JavaField> primaryKeys,
                            YdbTableHint hint, List<Schema.Index> globalIndexes, Schema.TtlModifier ttlModifier,
                            List<Schema.Changefeed> changefeeds) {
        TableDescription.Builder builder = TableDescription.newBuilder();
        columns.forEach(c -> {
            ValueProtos.Type.PrimitiveTypeId yqlType = YqlType.of(c).getYqlType();
            int yqlTypeNumber = yqlType.getNumber();
            PrimitiveType.Id primitiveTypeId = Stream.of(PrimitiveType.Id.values())
                    .filter(id -> id.getNumId() == yqlTypeNumber)
                    .findFirst()
                    .orElseThrow(() -> new CreateTableException(String.format("Can't create table '%s'%n"
                            + "Can't find yql primitive type '%s' in YDB SDK", name, yqlType)));
            builder.addNullableColumn(c.getName(), PrimitiveType.of(primitiveTypeId));
        });
        List<String> primaryKeysNames = primaryKeys.stream().map(Schema.JavaField::getName).collect(toList());
        builder.setPrimaryKeys(primaryKeysNames);
        globalIndexes.forEach(index -> builder.addGlobalIndex(index.getIndexName(), index.getFieldNames()));

        Session session = sessionManager.getSession();
        try {
            String tableDirectory = tableDirectory(tablespace + name);
            if (!isNullOrEmpty(tableDirectory)) {
                mkdirs(tableDirectory);
            }
            CreateTableSettings tableSettings = new CreateTableSettings();
            if (hint != null) {
                PartitioningPolicy partitioningPolicy = hint.getPartitioningPolicy();
                if (partitioningPolicy != null) {
                    tableSettings.setPartitioningPolicy(partitioningPolicy);
                }
                YdbTableHint.TablePreset tablePreset = hint.getTablePreset();
                if (tablePreset != null) {
                    tableSettings.setPresetName(tablePreset.getTablePresetName());
                }
                PartitioningSettings partitioningSettings = hint.getPartitioningSettings();
                if (partitioningSettings != null) {
                    tableSettings.setPartitioningSettings(partitioningSettings);
                }
            }
            if (ttlModifier != null) {
                TtlSettings ttlSettings = new TtlSettings(ttlModifier.getFieldName(), ttlModifier.getInterval());
                tableSettings.setTtlSettings(ttlSettings);
            }
            Status status = session
                    .createTable(tablespace + name, builder.build(), tableSettings)
                    .join();
            if (status.getCode() != com.yandex.ydb.core.StatusCode.SUCCESS) {
                throw new CreateTableException(String.format("Can't create table %s: %s", name, status));
            }

            if (!changefeeds.isEmpty()) {
                // Currently only one changefeed can be added by one alter operation
                for (var changefeed : changefeeds) {
                    AlterTableSettings alterTableSettings = new AlterTableSettings();
                    alterTableSettings.addChangefeed(new Changefeed(
                            changefeed.getName(),
                            Changefeed.Mode.valueOf(changefeed.getMode().name()),
                            Changefeed.Format.valueOf(changefeed.getFormat().name()),
                            changefeed.isVirtualTimestampsEnabled(),
                            changefeed.getRetentionPeriod(),
                            changefeed.isInitialScanEnabled()
                    ));
                    status = session.alterTable(tablespace + name, alterTableSettings).join();
                    if (status.getCode() != com.yandex.ydb.core.StatusCode.SUCCESS) {
                        throw new CreateTableException(String.format("Can't alter table %s: %s", name, status));
                    }
                }
            }
        } finally {
            sessionManager.release(session);
        }
    }

    public Table describeTable(String name, List<EntitySchema.JavaField> columns, List<EntitySchema.JavaField> primaryKeys,
                               List<EntitySchema.Index> indexes) {
        Set<String> primaryKeysNames = primaryKeys.stream().map(Schema.JavaField::getName).collect(toSet());
        List<Column> ydbColumns = columns.stream()
                .map(c -> {
                    String columnName = c.getName();
                    String simpleType = YqlType.of(c).getYqlType().name();
                    boolean isPrimaryKey = primaryKeysNames.contains(columnName);
                    return new Column(columnName, simpleType, isPrimaryKey);
                })
                .collect(toList());
        List<Index> ydbIndexes = indexes.stream()
                .map(i -> new Index(i.getIndexName(), i.getFieldNames()))
                .collect(toList());
        return new Table(tablespace + name, ydbColumns, ydbIndexes);
    }

    public boolean hasTable(String name) {
        return hasPath(tablespace + name);
    }

    public void dropTable(String name) {
        dropTablePath(tablespace + name);
    }

    @SneakyThrows
    private void dropTablePath(String table) {
        Session session = sessionManager.getSession();
        try {
            Status status = session.dropTable(table).join();
            if (!status.isSuccess()) {
                log.error("Table " + table + " not deleted");
                throw new DropTableException(String.format("Can't drop table %s: %s", table, status));
            }
        } finally {
            sessionManager.release(session);
        }
    }

    public List<String> getTableNames() {
        return getTableNames(false);
    }

    public List<String> getTableNames(boolean recursive) {
        return tableStream(recursive)
                .map(DirectoryEntity::getName)
                .collect(toList());
    }

    public List<String> getDirectoryNames() {
        return listDirectory(tablespace).stream()
                .filter(e -> e.getType() == SchemeOperationProtos.Entry.Type.DIRECTORY)
                .map(DirectoryEntity::getName)
                .collect(toList());
    }

    public List<Table> getTables() {
        return getTables(false);
    }

    public List<Table> getTables(boolean recursive) {
        return getTables(tablespace, recursive);
    }

    public List<Table> getTables(String basePath) {
        return getTables(basePath, false);
    }

    public List<Table> getTables(String basePath, boolean recursive) {
        String canonicalBasePath = canonicalTablespace(basePath);
        return tableStream(canonicalBasePath, recursive)
                .map(e -> describeTableInternal(canonicalBasePath + e.getName()))
                .collect(toList());
    }

    private Stream<DirectoryEntity> tableStream(boolean recursive) {
        return tableStream(tablespace, recursive);
    }

    private Stream<DirectoryEntity> tableStream(String canonicalPath, boolean recursive) {
        return tables(canonicalPath, "", recursive).stream();
    }

    private List<DirectoryEntity> tables(String canonicalPath, String subDir, boolean recursive) {
        String tableDir = YdbPaths.join(canonicalPath, subDir);
        List<DirectoryEntity> result = new ArrayList<>();
        for (DirectoryEntity entity : listDirectory(tableDir)) {
            if (recursive && entity.getType() == SchemeOperationProtos.Entry.Type.DIRECTORY) {
                result.addAll(tables(canonicalPath, YdbPaths.join(subDir, entity.getName()), true));
            } else if (entity.getType() == SchemeOperationProtos.Entry.Type.TABLE) {
                result.add(entity.withName(YdbPaths.join(subDir, entity.getName())));
            }
        }
        return result;
    }

    public Table describeTable(String tableName) {
        return describeTableInternal(tablespace + tableName);
    }

    @NonNull
    @SneakyThrows
    private Table describeTableInternal(String path) {
        Session session = sessionManager.getSession();
        Result<TableDescription> result;
        try {
            result = session.describeTable(path).join();
        } finally {
            sessionManager.release(session);
        }
        if (SCHEME_ERROR == result.getCode() && DEFAULT_ERROR.isContainedIn(result.getIssues())) {
            throw new YdbSchemaPathNotFoundException(result.toString());
        } else if (!result.isSuccess()) {
            throw new YdbRepositoryException("Can't describe table '" + path + "': " + result);
        }

        TableDescription table = result.expect("");
        return new Table(
                path,
                table.getColumns().stream()
                        .map(c -> {
                            String columnName = c.getName();
                            String simpleType = ((OptionalType) c.getType()).getItemType().toPb().getTypeId().name();
                            boolean isPrimaryKey = table.getPrimaryKeys().contains(columnName);
                            return new Column(columnName, simpleType, isPrimaryKey);
                        })
                        .collect(toList()),
                table.getIndexes().stream()
                        .filter(i -> i.getType() == TableIndex.Type.GLOBAL)
                        .map(i -> new Index(i.getName(), i.getColumns()))
                        .collect(toList())
        );
    }

    public void removeTablespace() {
        removeTablespace(tablespace);
    }

    public void removeDirectoryRecursive(String directory) {
        removeTablespace(tablespace + directory + "/");
    }

    private void removeTablespace(String tablespace) {
        listDirectory(tablespace).forEach(e -> {
            switch (e.getType()) {
                case DIRECTORY:
                    removeTablespace(tablespace + e.getName() + "/");
                    break;
                case TABLE:
                    try {
                        dropTablePath(tablespace + e.getName());
                    } catch (Exception ex) {
                        log.error("Can't remove table " + e.getName(), ex);
                    }
                    break;
            }
        });

        Status res = schemeClient.removeDirectory(tablespace.substring(0, tablespace.length() - 1))
                .join();
        log.trace(res.toString());
        if (!res.isSuccess()) {
            log.error("Can't remove directory " + tablespace);
        }
    }

    public void snapshot(String snapshotPath) throws SnapshotCreateException {
        mkdirs(canonicalRootDir(snapshotPath));
        getTableNames().forEach(tableName -> copyTable(tablespace + tableName, snapshotPath + tableName));

        getDirectoryNames().stream()
                .filter(name -> !name.startsWith("."))
                .filter(name -> !isSnapshotDirectory(name))
                .forEach(dirName -> {
                    String curTablespace = tablespace;

                    setTablespace(tablespace + dirName + "/");
                    snapshot(snapshotPath + dirName + "/");

                    setTablespace(curTablespace);
                });
    }

    public boolean isSnapshotDirectory(String name) {
        return name.startsWith(".snapshot-");
    }

    @SneakyThrows
    protected void copyTable(String source, String destination) {
        Session session = sessionManager.getSession();
        try {
            Status status = session.copyTable(source, destination).join();
            if (!status.isSuccess()) {
                throw new SnapshotCreateException(String.format("Error while copying from %s to %s: %s", source, destination, status));
            }
        } finally {
            sessionManager.release(session);
        }
    }

    @SneakyThrows
    private List<DirectoryEntity> listDirectory(String directory) {
        ListDirectoryResult result = schemeClient.listDirectory(directory).join()
                .expect("Can't list directory " + directory);

        List<String> errors = new ArrayList<>();
        List<DirectoryEntity> tables = result.getChildren().stream()
                .filter(entry -> {
                    switch (entry.getType()) {
                        case DIRECTORY:
                        case TABLE:
                            return true;
                        case COLUMN_STORE:
                            return false;
                        default:
                            errors.add(String.format("Unexpected entry type (%s:%s) in directory %s", entry.getType(), entry.getName(), directory));
                            return false;
                    }
                })
                .map(tEntry -> new DirectoryEntity(tEntry.getType(), tEntry.getName()))
                .collect(toList());
        if (!errors.isEmpty()) {
            throw new IllegalStateException(String.join(String.format("%n"), errors));
        }
        return tables;
    }

    protected void mkdirs(String dir) {
        if (!dir.isEmpty() && !hasPath(dir)) {
            Status status = schemeClient.makeDirectories(dir).join();
            if (!status.isSuccess()) {
                throw new IllegalStateException(String.format("Unable to create dir %s: %s", dir, status));
            }
        }
    }

    protected boolean hasPath(String path) {
        return schemeClient.describePath(path).join().isSuccess();
    }

    @Value
    private static class DirectoryEntity {
        SchemeOperationProtos.Entry.Type type;
        @With
        String name;
    }

    @Value
    public static class Table {
        String name;
        List<Column> columns;
        List<Index> indexes;

        @java.beans.ConstructorProperties({"name", "columns", "indexes"})
        private Table(String name, List<Column> columns, List<Index> indexes) {
            this.name = name;
            this.columns = columns;
            this.indexes = indexes;
        }
    }

    @Value
    @RequiredArgsConstructor(access = PRIVATE)
    public static class Column {
        String name;
        String type;
        boolean primary;
    }

    @Value
    @RequiredArgsConstructor(access = PRIVATE)
    public static class Index {
        String name;
        List<String> columns;
    }
}
