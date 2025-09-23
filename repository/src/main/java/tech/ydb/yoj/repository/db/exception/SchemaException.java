package tech.ydb.yoj.repository.db.exception;

@SuppressWarnings("checkstyle:LeftCurly")
public sealed abstract class SchemaException
        extends RepositoryException
        permits
            CreateTableException,
            DropTableException, PathNotFoundException,
            SnapshotCreateException, GenericSchemaException
{
    public SchemaException(String message) {
        super(message);
    }
}
