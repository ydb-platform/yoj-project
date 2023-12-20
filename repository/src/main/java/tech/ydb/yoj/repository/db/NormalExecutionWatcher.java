package tech.ydb.yoj.repository.db;

import java.util.function.Supplier;

public class NormalExecutionWatcher {
    // Is set to 'true' iff the last statement executed with exception.
    // This state will affect endSession work: it will either commit or rollback in DB.
    private boolean lastStatementCompletedExceptionally = false;

    public boolean hasLastStatementCompletedExceptionally() {
        return lastStatementCompletedExceptionally;
    }

    public void execute(Runnable command) {
        execute((Supplier<Void>) () -> {
            command.run();
            return null;
        });
    }

    public <RESULT> RESULT execute(Supplier<RESULT> command) {
        lastStatementCompletedExceptionally = true;
        RESULT res = command.get();
        lastStatementCompletedExceptionally = false;
        return res;
    }
}
