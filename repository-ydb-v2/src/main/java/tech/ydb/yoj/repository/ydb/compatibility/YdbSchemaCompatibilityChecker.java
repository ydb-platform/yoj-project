package tech.ydb.yoj.repository.ydb.compatibility;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import lombok.With;
import org.apache.commons.text.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.core.StatusCode;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.yoj.databind.DbType;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.TableDescriptor;
import tech.ydb.yoj.repository.ydb.YdbRepository;
import tech.ydb.yoj.repository.ydb.client.YdbPaths;
import tech.ydb.yoj.repository.ydb.client.YdbSchemaOperations;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public final class YdbSchemaCompatibilityChecker {
    private static final Logger log = LoggerFactory.getLogger(YdbSchemaCompatibilityChecker.class);

    private final List<TableDescriptor<?>> descriptors;
    private final Config config;
    private final YdbRepository repository;

    private final List<String> shouldExecuteMessages = new ArrayList<>();
    private final List<String> canExecuteMessages = new ArrayList<>();
    private final List<String> incompatibleMessages = new ArrayList<>();

    public YdbSchemaCompatibilityChecker(YdbRepository repository, List<TableDescriptor<?>> descriptors) {
        this(repository, descriptors, Config.DEFAULT);
    }

    public YdbSchemaCompatibilityChecker(List<Class<? extends Entity>> entities, YdbRepository repository) {
        this(entities, repository, Config.DEFAULT);
    }

    public YdbSchemaCompatibilityChecker(List<Class<? extends Entity>> entities, YdbRepository repository, Config config) {
        this(repository, toDescriptors(entities), config);
    }

    public YdbSchemaCompatibilityChecker(YdbRepository repository, List<TableDescriptor<?>> descriptors, Config config) {
        this.descriptors = descriptors;
        this.config = config;
        this.repository = repository;
    }

    @VisibleForTesting
    public List<String> getShouldExecuteMessages() {
        return List.copyOf(shouldExecuteMessages);
    }

    @VisibleForTesting
    public List<String> getCanExecuteMessages() {
        return List.copyOf(canExecuteMessages);
    }

    public void run() {
        shouldExecuteMessages.clear();
        canExecuteMessages.clear();

        Map<String, YdbSchemaOperations.Table> tablesFromSource = generateSchemeFromCode();
        Set<String> tableDirectories = tablesFromSource.keySet().stream()
                .map(YdbPaths::tableDirectory)
                .filter(Objects::nonNull)
                .collect(toSet());

        List<YdbSchemaOperations.Table> realTables = new ArrayList<>();
        for (String path : tableDirectories) {
            List<YdbSchemaOperations.Table> tables = getRealTables(path);

            realTables.addAll(tables);
        }

        checkCompatibility(tablesFromSource, realTables);

        if (canExecuteMessages.isEmpty() && shouldExecuteMessages.isEmpty() && incompatibleMessages.isEmpty()) {
            log.info("DB schema and code schema have no differences.");
            return;
        }

        if (!incompatibleMessages.isEmpty()) {
            log.error("DB schema and code have incompatible differences.\n"
                    + "{}", String.join("\n", incompatibleMessages));
            throw new IllegalStateException("Code schema is not compatible with DB schema");
        }

        if (!canExecuteMessages.isEmpty()) {
            var ddl = config.useBuilderDDLSyntax
                    ? String.join(",\n", canExecuteMessages)
                    : "--!syntax_v1\n" + String.join("\n", canExecuteMessages);

            BiConsumer<String, String> logConsumer = config.warnOnMinorDifferences ? log::warn : log::info;
            logConsumer.accept("DB schema and code schema have minor differences.\n"
                            + "You can execute below commands at any time after successful deployment of the code:\n"
                            + "{}",
                    ddl
            );
        }

        if (!shouldExecuteMessages.isEmpty()) {
            var ddl = config.useBuilderDDLSyntax
                    ? String.join(",\n", shouldExecuteMessages)
                    : "--!syntax_v1\n" + String.join("\n", shouldExecuteMessages);
            log.error("DB schema and code schema have major differences.\n"
                            + "You must execute below commands before deploying the code:\n"
                            + "{}",
                    ddl
            );
            throw new IllegalStateException("Code schema is not compatible with DB schema");
        }
    }

    private List<YdbSchemaOperations.Table> getRealTables(String path) {
        try {
            return repository.getSchemaOperations().getTables(path);
        } catch (UnexpectedResultException e) {
            // SCHEME_ERROR means that path not found => don't have tables
            if (e.getStatus().getCode() == StatusCode.SCHEME_ERROR) {
                return List.of();
            }
            throw e;
        }
    }

    private Map<String, YdbSchemaOperations.Table> generateSchemeFromCode() {
        return descriptors.stream()
                .map(this::tableForEntity)
                .collect(toMap(YdbSchemaOperations.Table::getName, Function.identity()));
    }

    private YdbSchemaOperations.Table tableForEntity(TableDescriptor<?> c) {
        EntitySchema<?> schema = EntitySchema.of(c.entityType());
        return repository.getSchemaOperations().describeTable(
                c.tableName(),
                schema.flattenFields(),
                schema.flattenId(),
                schema.getGlobalIndexes(),
                schema.getTtlModifier()
        );
    }

    private void checkCompatibility(Map<String, YdbSchemaOperations.Table> tablesFromSource,
                                    List<YdbSchemaOperations.Table> actualTables) {
        Map<String, YdbSchemaOperations.Table> actualTableMap = actualTables.stream()
                .collect(toMap(YdbSchemaOperations.Table::getName, Function.identity()));

        if (!config.onlyExistingTables) {
            actualTables.stream()
                    .filter(table -> !tablesFromSource.containsKey(table.getName()))
                    .filter(table -> !containsPrefix(table.getName(), config.skipExistingTablesPrefixes))
                    .map(this::makeDeleteInstruction)
                    .forEach(canExecuteMessages::add);

            Consumer<String> tablesFromSourceAction = config.skipMissingTables ?
                    canExecuteMessages::add :
                    shouldExecuteMessages::add;
            tablesFromSource.values().stream()
                    .filter(table -> !actualTableMap.containsKey(table.getName()))
                    .map(this::makeCreateInstruction)
                    .forEach(tablesFromSourceAction);
        }

        Map<YdbSchemaOperations.Table, YdbSchemaOperations.Table> changedTables = tablesFromSource.values()
                .stream()
                .filter(table -> actualTableMap.containsKey(table.getName()))
                .filter(table -> {
                    YdbSchemaOperations.Table actualTable = actualTableMap.get(table.getName());
                    Set<YdbSchemaOperations.Column> actualColumns = new HashSet<>(actualTable.getColumns());
                    Set<YdbSchemaOperations.Column> requiredColumns = new HashSet<>(table.getColumns());
                    return !actualColumns.equals(requiredColumns);
                })
                .collect(toMap(t -> actualTableMap.get(t.getName()), Function.identity()));
        changedTables.forEach(this::makeMigrationTableInstruction);

        Map<YdbSchemaOperations.Table, YdbSchemaOperations.Table> changedTableIndexes = tablesFromSource.values()
                .stream()
                .filter(table -> actualTableMap.containsKey(table.getName()))
                .filter(table -> {
                    YdbSchemaOperations.Table actualTable = actualTableMap.get(table.getName());
                    Set<YdbSchemaOperations.Index> actualIndexes = new HashSet<>(actualTable.getIndexes());
                    Set<YdbSchemaOperations.Index> requiredIndexes = new HashSet<>(table.getIndexes());
                    return !actualIndexes.equals(requiredIndexes);
                })
                .collect(toMap(t -> actualTableMap.get(t.getName()), Function.identity()));
        changedTableIndexes.forEach(this::makeMigrationTableIndexInstructions);

        Map<YdbSchemaOperations.Table, YdbSchemaOperations.Table> changedTableTtlModifiers = tablesFromSource.values()
                .stream()
                .filter(table -> actualTableMap.containsKey(table.getName()))
                .filter(table -> {
                    YdbSchemaOperations.Table actualTable = actualTableMap.get(table.getName());
                    YdbSchemaOperations.TtlModifier actualTtlModifier = actualTable.getTtlModifier();
                    YdbSchemaOperations.TtlModifier reqiredTtlModifier = table.getTtlModifier();
                    return !Objects.equals(actualTtlModifier, reqiredTtlModifier);
                })
                .collect(toMap(t -> actualTableMap.get(t.getName()), Function.identity()));
        changedTableTtlModifiers.forEach(this::makeMigrationTtlInstructions);
    }

    // FIXME: Style: Use Escaper from Guava here
    private static String javaLiteral(String s) {
        return "\"" + StringEscapeUtils.escapeJava(s) + "\"";
    }

    private static String builderDDLTableNameLiteral(YdbSchemaOperations.Table table) {
        var name = table.getName();
        var lastSlashIdx = name.lastIndexOf("/");
        if (lastSlashIdx >= 0) {
            name = name.substring(lastSlashIdx + 1);
        }
        return javaLiteral(name);
    }

    private String makeDeleteInstruction(YdbSchemaOperations.Table table) {
        if (config.useBuilderDDLSyntax) {
            return "DDLQuery.dropTable(" + builderDDLTableNameLiteral(table) + ")";
        } else {
            return "DROP TABLE `" + table.getName() + "`;";
        }
    }

    private String makeCreateInstruction(YdbSchemaOperations.Table table) {
        if (config.useBuilderDDLSyntax) {
            return "DDLQuery.createTable(" + builderDDLTableNameLiteral(table) + ")\n" +
                    "\t.table(TableDescription.newBuilder()\n" +
                    builderDDLColumns(table) +
                    builderDDLPrimaryKey(table) +
                    builderDDLIndexes(table) +
                    "\t\t.build())\n" +
                    "\t.build()";
        } else {
            return "CREATE TABLE `" + table.getName() + "` (\n" +
                    columns(table) + ",\n" +
                    "\tPRIMARY KEY(" + primaryKey(table) + ")" +
                    indexes(table) +
                    ");";
        }
    }

    private String makeDropColumn(YdbSchemaOperations.Table table, YdbSchemaOperations.Column c) {
        if (config.useBuilderDDLSyntax) {
            return "DDLQuery.dropColumn(" + builderDDLTableNameLiteral(table) + ", " +
                    javaLiteral(c.getName()) + ")";
        } else {
            return String.format("ALTER TABLE `%s` DROP COLUMN `%s`;", table.getName(), c.getName());
        }
    }

    private String makeAddColumn(YdbSchemaOperations.Table table, YdbSchemaOperations.Column c) {
        if (config.useBuilderDDLSyntax) {
            return "DDLQuery.addColumn(" + builderDDLTableNameLiteral(table) + ", " +
                    javaLiteral(c.getName()) + ", " +
                    typeToDDL(c.getType()) + ")";
        } else {
            return String.format("ALTER TABLE `%s` ADD COLUMN `%s` %s;", table.getName(), c.getName(), c.getType());
        }
    }

    private static String builderDDLPrimaryKey(YdbSchemaOperations.Table table) {
        return "\t\t.setPrimaryKeys(" + table.getColumns().stream()
                .filter(YdbSchemaOperations.Column::isPrimary)
                .map(c -> javaLiteral(c.getName()))
                .collect(joining(", ")) + ")\n";
    }

    private static String builderDDLIndexes(YdbSchemaOperations.Table table) {
        return table.getIndexes().stream()
                .map(idx -> (idx.isAsync() ? "\t\t.addGlobalAsyncIndex(" : "\t\t.addGlobalIndex(") + javaLiteral(idx.getName()) + ", " +
                        idx.getColumns().stream()
                                .map(YdbSchemaCompatibilityChecker::javaLiteral)
                                .collect(joining(", "))
                        + ")\n")
                .collect(joining(""));
    }

    private static String builderDDLColumns(YdbSchemaOperations.Table table) {
        return table.getColumns().stream()
                .map(c -> "\t\t.addNullableColumn(" + javaLiteral(c.getName()) + ", " +
                        typeToDDL(c.getType()) + ")\n")
                .collect(joining(""));
    }

    private static String typeToDDL(String type) {
        if (type == null) {
            throw new IllegalArgumentException("Unknown db type: " + type);
        }
        return switch (DbType.valueOf(type)) {
            case DEFAULT -> throw new IllegalArgumentException("Unknown db type: " + type);
            case BOOL -> "PrimitiveType.bool()";
            case UINT8 -> "PrimitiveType.uint8()";
            case INT32 -> "PrimitiveType.int32()";
            case UINT32 -> "PrimitiveType.uint32()";
            case INT64 -> "PrimitiveType.int64()";
            case UINT64 -> "PrimitiveType.uint64()";
            case FLOAT -> "PrimitiveType.float32()";
            case DOUBLE -> "PrimitiveType.float64()";
            case DATE -> "PrimitiveType.date()";
            case DATETIME -> "PrimitiveType.datetime()";
            case TIMESTAMP -> "PrimitiveType.timestamp()";
            case INTERVAL -> "PrimitiveType.interval()";
            case STRING -> "PrimitiveType.string()";
            case UTF8 -> "PrimitiveType.utf8()";
            case JSON -> "PrimitiveType.json()";
            case JSON_DOCUMENT -> "PrimitiveType.jsonDocument()";
            case UUID -> "PrimitiveType.uuid()";
        };
    }

    private static String columns(YdbSchemaOperations.Table table) {
        return table.getColumns().stream()
                .map(c -> "\t`" + c.getName() + "` " + c.getType())
                .collect(joining(",\n"));
    }

    private static String primaryKey(YdbSchemaOperations.Table table) {
        return table.getColumns().stream()
                .filter(YdbSchemaOperations.Column::isPrimary)
                .map(c -> "`" + c.getName() + "`")
                .collect(joining(","));
    }

    private static String indexes(YdbSchemaOperations.Table table) {
        List<YdbSchemaOperations.Index> indexes = table.getIndexes();
        if (indexes.isEmpty()) {
            return "\n";
        }
        return ",\n" + indexes.stream()
            .map(idx -> "\t" + indexStatement(idx))
            .collect(Collectors.joining(",\n")) + "\n";
    }

    private static String indexStatement(YdbSchemaOperations.Index idx) {
        return String.format("INDEX `%s` GLOBAL %sON (%s)",
            idx.getName(), idx.isUnique() ? "UNIQUE " : idx.isAsync() ? "ASYNC " : "", indexColumns(idx.getColumns()));
    }

    private static String indexColumns(List<String> columns) {
        return columns.stream().map(c -> "`" + c + "`").collect(Collectors.joining(","));
    }

    private void makeMigrationTableInstruction(YdbSchemaOperations.Table from, YdbSchemaOperations.Table to) {
        Map<String, YdbSchemaOperations.Column> toColumns = to.getColumns().stream()
                .collect(toMap(YdbSchemaOperations.Column::getName, Function.identity()));
        Map<String, YdbSchemaOperations.Column> fromColumns = from.getColumns().stream()
                .collect(toMap(YdbSchemaOperations.Column::getName, Function.identity()));

        toColumns.values().stream()
                .filter(s -> !fromColumns.containsKey(s.getName()))
                .filter(YdbSchemaOperations.Column::isPrimary)
                .map(column -> cannotBeAlteredMessage(from, column, column.getName() + " is part of a table ID"))
                .forEach(incompatibleMessages::add);

        toColumns.values().stream()
                .filter(s -> !fromColumns.containsKey(s.getName()))
                .filter(not(YdbSchemaOperations.Column::isPrimary))
                .map(c -> makeAddColumn(from, c))
                .forEach(shouldExecuteMessages::add);

        fromColumns.values().stream()
                .filter(s -> !toColumns.containsKey(s.getName()))
                .filter(YdbSchemaOperations.Column::isPrimary)
                .map(column -> cannotBeAlteredMessage(from, column, column.getName() + " is part of a table ID"))
                .forEach(incompatibleMessages::add);

        fromColumns.values().stream()
                .filter(s -> !toColumns.containsKey(s.getName()))
                .filter(not(YdbSchemaOperations.Column::isPrimary))
                .map(c -> makeDropColumn(from, c))
                .forEach(canExecuteMessages::add);

        fromColumns.values().stream()
                .filter(column -> toColumns.containsKey(column.getName()))
                .filter(column -> !toColumns.get(column.getName()).equals(column))
                .map(column -> cannotBeAlteredMessage(from, column, columnDiff(column, toColumns.get(column.getName()))))
                .forEach(incompatibleMessages::add);
    }

    private String cannotBeAlteredMessage(YdbSchemaOperations.Table table, YdbSchemaOperations.Column column, String reason) {
        return "Altering column `" + table.getName() + "`." + column.getName() + " is impossible: " + reason + ".";
    }

    private void makeMigrationTableIndexInstructions(YdbSchemaOperations.Table from, YdbSchemaOperations.Table to) {
        Map<String, YdbSchemaOperations.Index> toIndexes = to.getIndexes().stream()
                .collect(toMap(YdbSchemaOperations.Index::getName, Function.identity()));
        Map<String, YdbSchemaOperations.Index> fromIndexes = from.getIndexes().stream()
                .collect(toMap(YdbSchemaOperations.Index::getName, Function.identity()));

        Function<YdbSchemaOperations.Index, String> createIndex = i ->
            String.format("ALTER TABLE `%s` ADD %s;", to.getName(), indexStatement(i));

        Function<YdbSchemaOperations.Index, String> dropIndex = i ->
                String.format("ALTER TABLE `%s` DROP INDEX `%s`;", from.getName(), i.getName());

        toIndexes.values().stream()
                .filter(i -> !fromIndexes.containsKey(i.getName()))
                .map(createIndex)
                .forEach(shouldExecuteMessages::add);

        fromIndexes.values().stream()
                .filter(i -> !toIndexes.containsKey(i.getName()))
                .map(dropIndex)
                .forEach(canExecuteMessages::add);

        fromIndexes.values().stream()
                .filter(i -> toIndexes.containsKey(i.getName()))
                .filter(i -> !toIndexes.get(i.getName()).equals(i))
                .map(i -> {
                    YdbSchemaOperations.Index newIndex = toIndexes.get(i.getName());
                    return String.format("Altering index `%s`.%s is impossible: columns are changed: %s --> %s.%n%s%n%s",
                            from.getName(), i.getName(), i.getColumns(), newIndex.getColumns(),
                            dropIndex.apply(i),
                            createIndex.apply(newIndex));
                })
                .forEach(shouldExecuteMessages::add);
    }

    private void makeMigrationTtlInstructions(YdbSchemaOperations.Table from, YdbSchemaOperations.Table to) {
        YdbSchemaOperations.TtlModifier toTtlModifier = to.getTtlModifier();

        if (toTtlModifier == null) {
            String dropOldTtl = "ALTER TABLE `%s` RESET (TTL);".formatted(from.getName());
            shouldExecuteMessages.add(dropOldTtl);
        }

        if (toTtlModifier != null) {
            String alterAddTtlTemplate = "ALTER TABLE `%s` SET (TTL = Interval(\"%s\") ON %s);";
            String ttlColumn = toTtlModifier.getDateTimeColumnName();
            Duration ttlDuration = Duration.ofSeconds(toTtlModifier.getExpireAfterSeconds());
            shouldExecuteMessages.add(alterAddTtlTemplate.formatted(to.getName(), ttlDuration, ttlColumn));
        }
    }

    private String columnDiff(YdbSchemaOperations.Column column, YdbSchemaOperations.Column newColumn) {
        if (column.isPrimary() != newColumn.isPrimary()) {
            return "primary_key changed: " + column.isPrimary() + " --> " + newColumn.isPrimary();
        }
        return "type changed: " + column.getType() + " --> " + newColumn.getType();
    }

    private boolean containsPrefix(String globalName, Set<String> prefixes) {
        if (prefixes.isEmpty()) {
            return false;
        }

        String tablespace = repository.getSchemaOperations().getTablespace();
        Preconditions.checkState(globalName.startsWith(tablespace), "valid global name must start with repository tablespace");
        String realName = globalName.substring(tablespace.length());
        return prefixes.stream()
                .anyMatch(realName::startsWith);
    }

    private static List<TableDescriptor<?>> toDescriptors(List<Class<? extends Entity>> entities) {
        List<TableDescriptor<?>> descriptors = new ArrayList<>();
        entities.forEach(e -> descriptors.add(TableDescriptor.from(EntitySchema.of(e))));
        return descriptors;
    }

    @Value
    @Builder
    @With
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Config {
        public static final Config DEFAULT = Config.builder().build();

        @Builder.Default
        boolean onlyExistingTables = false; // check only tables which exists in code and in DB
        @Builder.Default
        boolean skipMissingTables = false; // don't fail if table from code isn't found in DB
        @Builder.Default
        Set<String> skipExistingTablesPrefixes = Set.of(); // skip tables with certain prefixes that exists in DB, but not in code
        @Builder.Default
        boolean useBuilderDDLSyntax = false; // suggest db changes using Java builder syntax
        @Builder.Default
        boolean warnOnMinorDifferences = true; // log warn message on minor differences
    }
}
