package tech.ydb.yoj.repository.ydb.client;

import com.google.common.base.Preconditions;
import lombok.NonNull;
import tech.ydb.yoj.InternalApi;

import java.util.regex.Pattern;

@InternalApi
public final class YdbPaths {
    private static final Pattern DIRECTORY_PATTERN = Pattern.compile("[A-Za-z0-9-_][A-Za-z0-9-_.]{0,254}");
    private static final Pattern OBJECT_PATTERN = Pattern.compile("[A-Za-z0-9-_.]{1,255}");
    private static final Pattern ONLY_DOTS_PATTERN = Pattern.compile("[.]+");

    private YdbPaths() {
    }

    public static String canonicalTablespace(String tablespace) {
        Preconditions.checkArgument(tablespace.startsWith("/"), "tablespace must be an absolute path, but got: '%s'", tablespace);
        validatePath(tablespace, true);
        return tablespace.endsWith("/") ? tablespace : tablespace + "/";
    }

    public static String canonicalRootDir(String tablespace) {
        Preconditions.checkArgument(tablespace.startsWith("/"), "tablespace must be an absolute path, but got: '%s'", tablespace);
        validatePath(tablespace, true);
        return tablespace.endsWith("/") ? tablespace.substring(0, tablespace.length() - 1) : tablespace;
    }

    public static String canonicalDatabase(String database) {
        Preconditions.checkArgument(database.startsWith("/"), "database path must be absolute, but got: '%s'", database);
        validatePath(database, true);
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

    /**
     * Validates an absolute or relative YDB path to a non-system object
     * (disallowing leading {@code '.'} in directory names).
     *
     * @param path        YDB path to validate (either absolute or relative)
     * @param isDirectory {@code true} if this is a path to directory; {@code false} if this is a path to object
     * @see <a href="https://ydb.tech/docs/en/concepts/datamodel/cluster-namespace">Cluster Namespace and Object Naming</a>
     * @see #validateObjectName(String)
     * @see #validateDirectoryName(String)
     */
    public static String validatePath(@NonNull String path, boolean isDirectory) {
        Preconditions.checkArgument(!path.isEmpty(), "YDB path must not be empty");

        String[] segments = path.split("/", -1);
        int startInclusive = segments[0].isEmpty() ? 1 : 0;
        int endExclusive = segments[segments.length - 1].isEmpty() ? segments.length - 1 : segments.length;
        for (int i = startInclusive; i < endExclusive; i++) {
            String segment = segments[i];
            if (i != endExclusive - 1) {
                // Treat every non-final path segment as a directory-like thing:
                validateDirectoryName(segment);
            } else {
                if (isDirectory) {
                    validateDirectoryName(segment);
                } else {
                    validateObjectName(segment);
                }
            }
        }
        return path;
    }

    /**
     * Validates a YDB non-system directory local name (disallowing leading {@code '.'} in directory names).
     *
     * @see <a href="https://ydb.tech/docs/en/concepts/datamodel/cluster-namespace">Cluster Namespace and Object Naming</a>
     */
    public static void validateDirectoryName(@NonNull String directoryName) {
        int length = directoryName.length();
        Preconditions.checkArgument(length >= 1 && length <= 255,
                "YDB directory name length must be between 1 and 255, but got: %s", length);

        Preconditions.checkArgument(!ONLY_DOTS_PATTERN.matcher(directoryName).matches(),
                "YDB directory name must not contain only dots, but got: '%s'", directoryName);

        Preconditions.checkArgument(DIRECTORY_PATTERN.matcher(directoryName).matches(),
                "Not a valid YDB directory name, got: '%s'", directoryName);
    }

    /**
     * Validates a YDB object local name, e.g. a table name or index name, allowing leading {@code '.'} in object names.
     *
     * @see <a href="https://ydb.tech/docs/en/concepts/datamodel/cluster-namespace">Cluster Namespace and Object Naming</a>
     * @see #validatePath(String, boolean)
     */
    public static void validateObjectName(@NonNull String objectName) {
        int length = objectName.length();
        Preconditions.checkArgument(length >= 1 && length <= 255,
                "YDB object name length must be between 1 and 255, but got: %s", length);

        Preconditions.checkArgument(!ONLY_DOTS_PATTERN.matcher(objectName).matches(),
                "YDB object name must not contain only dots, but got: '%s'", objectName);

        Preconditions.checkArgument(OBJECT_PATTERN.matcher(objectName).matches(),
                "Not a valid YDB object name, got: '%s'", objectName);
    }
}
