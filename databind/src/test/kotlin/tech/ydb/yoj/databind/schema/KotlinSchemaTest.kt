package tech.ydb.yoj.databind.schema

import org.assertj.core.api.Assertions.assertThat
import org.junit.BeforeClass
import org.junit.Test
import java.util.Map.entry

class KotlinSchemaTest {
    @Test
    fun testRawPathField() {
        val field = schema!!.getField("entity1")

        assertThat(field.rawPath).isEqualTo("entity1")
    }

    @Test
    fun testRawPathSubField() {
        val field = schema!!.getField("entity1.entity2.entity3")

        assertThat(field.rawPath).isEqualTo("entity1.entity2.entity3")
    }

    @Test
    fun testRawPathLeafField() {
        val field = schema!!.getField("entity1.entity2.entity3.value")

        assertThat(field.rawPath).isEqualTo("entity1.entity2.entity3.value")
    }

    @Test
    fun testRawSubPathSubField() {
        val field = schema!!.getField("entity1.entity2.entity3")

        assertThat(field.getRawSubPath(1)).isEqualTo("entity2.entity3")
    }

    @Test
    fun testRawSubPathLeafFieldOnlyLead() {
        val field = schema!!.getField("entity1.entity2.entity3.value")

        assertThat(field.getRawSubPath(3)).isEqualTo("value")
    }

    @Test
    fun testRawSubPathLeafFieldEqualNesting() {
        val field = schema!!.getField("entity1.entity2.entity3.value")

        assertThat(field.getRawSubPath(4)).isEmpty()
    }

    @Test
    fun testRawSubPathLeafFieldExceedsNesting() {
        val field = schema!!.getField("entity1.entity2.entity3.value")

        assertThat(field.getRawSubPath(10)).isEmpty()
    }

    @Test
    fun testIsFlatTrue() {
        assertThat(schema!!.getField("flatEntity").isFlat).isTrue()
    }

    @Test
    fun testIdsFlatFalse() {
        assertThat(schema!!.getField("twoFieldEntity").isFlat).isFalse()
    }

    @Test
    fun testIsFlatFalseForNotFlat() {
        assertThat(schema!!.getField("notFlatEntity").isFlat).isFalse()
    }

    @Test
    fun testPrivateProp() {
        val ue = UberEntity(null, null, null, null, PrivatePropEntity(42))
        assertThat(schema!!.flatten(ue)).containsOnly(entry("privatePropEntity_privateProp", 42))
        assertThat(schema!!.newInstance(mapOf("privatePropEntity_privateProp" to 42))).isEqualTo(ue)
    }

    private class TestSchema<T>(entityType: Class<T>) : Schema<T>(entityType)

    private data class UberEntity(
        val entity1: Entity1?,
        val flatEntity: FlatEntity?,
        val twoFieldEntity: TwoFieldEntity?,
        val notFlatEntity: NotFlatEntity?,
        val privatePropEntity: PrivatePropEntity
    )

    private data class Entity1(val entity2: Entity2)

    private data class Entity2(val entity3: Entity3)

    private data class Entity3(val value: Int)

    private data class FlatEntity(val entity1: Entity1)

    private data class TwoFieldEntity(
        val entity1: Entity1,
        val boolValue: Boolean,
    )

    private data class NotFlatEntity(
        val twoFieldEntity: TwoFieldEntity,
        val otherTwoFieldEntity: TwoFieldEntity,
    )

    private data class PrivatePropEntity(
        private val privateProp: Int
    )

    companion object {
        private var schema: Schema<UberEntity>? = null

        @JvmStatic
        @BeforeClass
        fun setUpClass() {
            schema = TestSchema(
                UberEntity::class.java
            )
        }
    }
}
