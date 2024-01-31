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
            chronology = Chronology(now, now),
            login = "logIn",
            description = null
        )
        val accountSnapshot = Account.Snapshot(
            id = Account.Snapshot.Id(account.id, 1L),
            entity = account,
            meta = ChangeMetadata(ChangeMetadata.ChangeKind.CREATE, now, 1L)
        )

        val schema = EntitySchema.of(Account.Snapshot::class.java)

        val flattened = schema.flatten(accountSnapshot)
        assertThat(flattened).isEqualTo(mapOf(
            "id_id" to "hahaha",
            "id_version" to 1L,
            "entity_id" to "hahaha",
            "entity_version" to 1L,
            "entity_chronology_createdAt" to now,
            "entity_chronology_updatedAt" to now,
            "entity_login" to "logIn",
            "meta_kind" to ChangeMetadata.ChangeKind.CREATE,
            "meta_time" to now,
            "meta_version" to 1L
        ))
        assertThat(schema.newInstance(flattened)).isEqualTo(accountSnapshot)
    }
}
