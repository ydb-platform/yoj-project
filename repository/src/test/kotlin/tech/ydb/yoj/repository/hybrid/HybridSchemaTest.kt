package tech.ydb.yoj.repository.hybrid

import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import tech.ydb.yoj.repository.db.EntitySchema
import java.time.Instant

/**
 * Test for Schema using both Kotlin data classes and JVM records.
 */
class HybridSchemaTest {
    @Test
    fun testIdFlattening() {
        val now = Instant.now()
        val account = Account(
            id = Account.Id("hahaha"),
            version = 1L,
            login = "logIn"
        )
        val accountSnapshot = Account.Snapshot(
            id = Account.Snapshot.Id(account.id, 1L),
            account = account,
            meta = AccountSnapshotMetadata(now, "Created account '${account.login}'")
        )

        val schema = EntitySchema.of(Account.Snapshot::class.java)

        val flattened = schema.flatten(accountSnapshot)
        assertThat(flattened).isEqualTo(mapOf(
            "id_id" to "hahaha",
            "id_version" to 1L,
            "account_id" to "hahaha",
            "account_version" to 1L,
            "account_login" to "logIn",
            "meta_at" to now,
            "meta_description" to "Created account 'logIn'"
        ))
        assertThat(schema.newInstance(flattened)).isEqualTo(accountSnapshot)
    }

    @Test
    fun json() {
        val op = Job(
            id = Job.Id("smth"),
            args = JobArgs("abc")
        )

        val schema = EntitySchema.of(Job::class.java)

        val flattened = schema.flatten(op)
        assertThat(flattened).isEqualTo(mapOf(
            "id" to "smth",
            "args" to JobArgs("abc")
        ))
        assertThat(schema.newInstance(flattened)).isEqualTo(op)
    }
}
