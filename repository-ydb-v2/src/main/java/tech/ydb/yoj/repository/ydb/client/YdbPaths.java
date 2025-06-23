package tech.ydb.yoj.repository.ydb.client;

import com.google.common.base.Preconditions;
import tech.ydb.yoj.InternalApi;

@InternalApi
public final class YdbPaths {
    private YdbPaths() {
    }

    public static String canonicalTablespace(String tablespace) {
        Preconditions.checkArgument(tablespace.startsWith("/"), "tablespace must be an absolute path, but got: '%s'", tablespace);
        return tablespace.endsWith("/") ? tablespace : tablespace + "/";
    }

    public static String canonicalRootDir(String tablespace) {
        Preconditions.checkArgument(tablespace.startsWith("/"), "tablespace must be an absolute path, but got: '%s'", tablespace);
        return tablespace.endsWith("/") ? tablespace.substring(0, tablespace.length() - 1) : tablespace;
    }

    public static String canonicalDatabase(String database) {
        Preconditions.checkArgument(database.startsWith("/"), "database path must be absolute, but got: '%s'", database);
        return database.endsWith("/") ? database.substring(0, database.length() - 1) : database;
    }

    public static String join(String parent, String child) {
        return parent.isEmpty() ? child : (parent.endsWith("/") ? parent : parent + "/") + child;
    }

    public static String tableDirectory(String tablePath) {
        if (!tablePath.contains("/")) {
            return null;
        }
        return tablePath.substring(0, tablePath.lastIndexOf("/"));
    }
}
