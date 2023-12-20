package tech.ydb.yoj.repository.ydb.client;

public final class YdbPaths {
    private YdbPaths() {
    }

    public static String canonicalTablespace(String tablespace) {
        return tablespace.endsWith("/") ? tablespace : tablespace + "/";
    }

    public static String canonicalRootDir(String tablespace) {
        return tablespace.endsWith("/") ? tablespace.substring(0, tablespace.length() - 1) : tablespace;
    }

    public static String canonicalDatabase(String database) {
        return database == null ? null : database.endsWith("/") ? database.substring(0, database.length() - 1) : database;
    }

    static String join(String parent, String child) {
        return parent.isEmpty() ? child : canonicalTablespace(parent) + child;
    }

    public static String tableDirectory(String tablePath) {
        if (!tablePath.contains("/")) {
            return null;
        }
        return tablePath.substring(0, tablePath.lastIndexOf("/"));
    }
}
