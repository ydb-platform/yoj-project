package tech.ydb.yoj.repository.hybrid

import tech.ydb.yoj.repository.db.Entity

data class Account(
    private val id: Id,
    private val version: Long,
    val login: String,
) : Entity<Account> {
    override fun getId() = id

    data class Id(val value: String) : Entity.Id<Account>

    data class Snapshot(
        private val id: Id,
        private val account: Account?,
        val meta: AccountSnapshotMetadata
    ) : Entity<Snapshot> {
        override fun getId() = id

        data class Id(
            private val id: Account.Id,
            private val version: Long
        ) : Entity.Id<Snapshot>
    }
}
