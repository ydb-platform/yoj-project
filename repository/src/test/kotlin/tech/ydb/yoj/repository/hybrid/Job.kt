package tech.ydb.yoj.repository.hybrid

import tech.ydb.yoj.databind.schema.Column
import tech.ydb.yoj.repository.db.Entity

data class Job(
    private val id: Id,
    @Column(flatten = false) val args: JobArgs?,
) : Entity<Job> {
    override fun getId() = id

    data class Id(val value: String) : Entity.Id<Job>
}
