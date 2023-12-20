package tech.ydb.yoj.repository.ydb.exception;

public class ResultTruncatedException extends YdbRepositoryException {
    public ResultTruncatedException(String message, Object request, Object response) {
        super(message, request, response);
    }
}
