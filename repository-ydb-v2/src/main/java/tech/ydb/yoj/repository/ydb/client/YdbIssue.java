package tech.ydb.yoj.repository.ydb.client;

import lombok.RequiredArgsConstructor;
import tech.ydb.core.Issue;
import tech.ydb.yoj.InternalApi;

import static lombok.AccessLevel.PRIVATE;

@InternalApi
@RequiredArgsConstructor(access = PRIVATE)
public enum YdbIssue {
    DEFAULT_ERROR(0),
    CONSTRAINT_VIOLATION(2012);

    private final int issueCode;

    public boolean isContainedIn(Issue[] messages) {
        for (Issue message : messages) {
            if (issueCode == message.getCode() || isContainedIn(message.getIssues())) {
                return true;
            }
        }
        return false;
    }
}
