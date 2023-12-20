package tech.ydb.yoj.repository.ydb.client;

import com.yandex.ydb.core.Issue;
import lombok.RequiredArgsConstructor;

import static lombok.AccessLevel.PRIVATE;

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
