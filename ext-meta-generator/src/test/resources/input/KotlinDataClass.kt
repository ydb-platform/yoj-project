package input

import tech.ydb.yoj.databind.schema.Column
import tech.ydb.yoj.databind.schema.Table
import tech.ydb.yoj.repository.db.Entity

@Table(name = "my_table")
data class KotlinDataClass(
        @Column(name = "id")
        private val id: Id,

        @Column(name = "cloud_id")
        val field1: String,

        @Column(name = "folder_id")
        val field2: String,

        @Column(name = "status")
        val status: SomeEnum,

        val data: NotNestedClass,

) : Entity<KotlinDataClass> {
    override fun getId() = id

    data class Id(
            val nestedField1: String,
            val nestedField2: String
    ) : Entity.Id<KotlinDataClass>
}

data class NotNestedClass(
        @Column(name = "issued_at")
        val issuedAt: Long,
       @Column(name = "algorithm")
        val algorithm: String?
)

enum class SomeEnum {
    STATUS_UNSPECIFIED,
    UNSIGNED,
    ACTIVE
}