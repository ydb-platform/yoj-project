package tech.ydb.yoj.repository.test.sample;

import lombok.Value;
import tech.ydb.yoj.repository.db.Table;
import tech.ydb.yoj.repository.test.sample.model.Team;

@Value
public final class TeamView implements Table.View {
    Team.Id id;
    Team.Id parentId;
}
