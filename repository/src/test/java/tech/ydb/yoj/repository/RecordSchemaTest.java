package tech.ydb.yoj.repository;

import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.Value;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import tech.ydb.yoj.databind.schema.Column;
import tech.ydb.yoj.databind.schema.ConstructionException;
import tech.ydb.yoj.databind.schema.Schema;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntityIdSchema;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.RecordEntity;
import tech.ydb.yoj.repository.db.Table.View;
import tech.ydb.yoj.repository.db.ViewSchema;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.entry;

public class RecordSchemaTest {
    private EntitySchema<SampleEntity> schema;
    private ViewSchema<SampleEntityView> viewSchema;
    private EntityIdSchema<SampleEntity.Id> idSchema;

    @Before
    public void setUp() {
        schema = EntitySchema.of(SampleEntity.class);
        idSchema = schema.getIdSchema();
        viewSchema = schema.getViewSchema(SampleEntityView.class);
    }

    @After
    public void tearDown() {
        schema = null;
        viewSchema = null;
        idSchema = null;
    }

    @Test
    public void find_field() {
        assertThat(schema.findField("id.zone")).isPresent();
        assertThat(schema.getField("id.zone").getName()).isEqualTo("id_zone");
        assertThat(schema.getField("id.zone").getType()).isEqualTo(String.class);
    }

    @Test
    public void find_field_in_id_schema() {
        assertThat(idSchema.findField("zone")).isPresent();
        assertThat(idSchema.getField("zone").getName()).isEqualTo("id_zone");
        assertThat(idSchema.getField("zone").getType()).isEqualTo(String.class);
    }

    @Test
    public void find_field_in_view_schema() {
        assertThat(viewSchema.findField("created")).isPresent();
        assertThat(viewSchema.getField("created").getName()).isEqualTo("created");
        assertThat(viewSchema.getField("created").getType()).isEqualTo(Instant.class);
    }

    @Test
    public void serialize_view_to_map() {
        assertThat(viewSchema.flatten(new SampleEntityView(
                Instant.EPOCH,
                Set.of("hello", "world"),
                "hi"
        ))).isEqualTo(Map.of(
                "name", "hi",
                "created", Instant.EPOCH,
                "tags", Set.of("hello", "world")
        ));
    }

    @Test
    public void deserialize_view_from_map() {
        var view = viewSchema.newInstance(Map.of(
                "name", "hi",
                "created", Instant.EPOCH,
                "tags", Set.of("hello", "world")
        ));
        assertThat(view).isEqualTo(new SampleEntityView(
                Instant.EPOCH,
                Set.of("hello", "world"),
                "hi"
        ));
    }

    @Test(expected = IllegalArgumentException.class)
    public void get_field_fails_on_nonexistent_field() {
        schema.getField("something.else");
    }

    @Test
    public void find_field_returns_Optional_empty_on_nonexistent_field() {
        assertThat(schema.findField("something.else")).isEmpty();
    }

    @Test
    public void find_field_returns_Optional_empty_on_bad_field_name() {
        assertThat(schema.findField("123helloworld")).isEmpty();
    }

    @Test
    public void find_field_returns_Optional_empty_on_empty_field_name() {
        assertThat(schema.findField("")).isEmpty();
    }

    @Test
    public void composite_id_schema() {
        assertThat(schema.flattenId().stream().map(Schema.JavaField::getName)).containsOnly("id_zone", "id_localId");
        assertThat(schema.flattenId(new SampleEntity.Id("ru-central-1", 100500L))).containsOnly(
                entry("id_zone", "ru-central-1"),
                entry("id_localId", 100500L)
        );
        assertThat(schema.findField("id.zone")).isPresent();
        assertThat(schema.findField("id.zone")).hasValueSatisfying(f -> assertThat(f.getName()).isEqualTo("id_zone"));
        assertThat(schema.findField("id.localId")).isPresent();
        assertThat(schema.findField("id.localId")).hasValueSatisfying(f -> assertThat(f.getName()).isEqualTo("id_localId"));

        EntityIdSchema<SampleEntity.Id> idSchema = EntityIdSchema.ofEntity(SampleEntity.class);
        assertThat(idSchema.flattenFieldNames()).containsOnly("id_zone", "id_localId");
        assertThat(idSchema.flatten(new SampleEntity.Id("ru-central-1", 100500L))).containsOnly(
                entry("id_zone", "ru-central-1"),
                entry("id_localId", 100500L)
        );
        assertThat(idSchema.findField("zone")).isPresent();
        assertThat(idSchema.findField("zone")).hasValueSatisfying(f -> assertThat(f.getName()).isEqualTo("id_zone"));
        assertThat(idSchema.findField("localId")).isPresent();
        assertThat(idSchema.findField("localId")).hasValueSatisfying(f -> assertThat(f.getName()).isEqualTo("id_localId"));
    }

    @Test
    public void simple_id_schema_is_flattened() {
        EntitySchema<SampleEntity2> schema = EntitySchema.of(SampleEntity2.class);
        assertThat(schema.flattenId()).hasSize(1);
        assertThat(schema.flattenId())
                .element(0)
                .satisfies(jf -> {
                    assertThat(jf.getName()).isEqualTo("id");
                    assertThat(jf.getType()).isEqualTo(String.class);
                });

        EntityIdSchema<SampleEntity2.FlatId> flatIdSchema = EntityIdSchema.ofEntity(SampleEntity2.class);
        assertThat(flatIdSchema.flattenFields()).hasSize(1);
        assertThat(flatIdSchema.flattenFields())
                .element(0)
                .satisfies(jf -> {
                    assertThat(jf.getName()).isEqualTo("id");
                    assertThat(jf.getType()).isEqualTo(String.class);
                });
    }

    @Test
    public void simple_id_schema_with_reference_is_flattened() {
        EntitySchema<SampleEntity3> schema = EntitySchema.of(SampleEntity3.class);
        assertThat(schema.flattenId()).hasSize(1);
        assertThat(schema.flattenId())
                .element(0)
                .satisfies(jf -> {
                    assertThat(jf.getName()).isEqualTo("id");
                    assertThat(jf.getType()).isEqualTo(String.class);
                });

        EntityIdSchema<SampleEntity3.IdWithOtherId> idWithRefSchema = EntityIdSchema.ofEntity(SampleEntity3.class);
        assertThat(idWithRefSchema.flattenFields()).hasSize(1);
        assertThat(idWithRefSchema.flattenFields())
                .element(0)
                .satisfies(jf -> {
                    assertThat(jf.getName()).isEqualTo("id");
                    assertThat(jf.getType()).isEqualTo(String.class);
                });
    }

    @Test
    public void simple_id_schema_with_reference_and_empty_subobject_is_flattened() {
        EntitySchema<SampleEntity4> schema = EntitySchema.of(SampleEntity4.class);
        assertThat(schema.flattenId()).hasSize(1);
        assertThat(schema.flattenId())
                .element(0)
                .satisfies(jf -> {
                    assertThat(jf.getName()).isEqualTo("id");
                    assertThat(jf.getType()).isEqualTo(String.class);
                });

        EntityIdSchema<SampleEntity4.IdWithOtherIdAndEmptySubobject> idWithRefSchema = EntityIdSchema.ofEntity(SampleEntity4.class);
        assertThat(idWithRefSchema.flattenFields()).hasSize(1);
        assertThat(idWithRefSchema.flattenFields())
                .element(0)
                .satisfies(jf -> {
                    assertThat(jf.getName()).isEqualTo("id");
                    assertThat(jf.getType()).isEqualTo(String.class);
                });
    }

    @Test
    public void allow_external_class_as_id_type() {
        EntitySchema<EntityWithTopLevelId> schema = EntitySchema.of(EntityWithTopLevelId.class);
        assertThat(schema.flattenId()).hasSize(1);
        assertThat(schema.flattenId(new TopLevelId("zzz"))).containsExactly(entry("id", "zzz"));

        EntityIdSchema<TopLevelId> idSchema = EntityIdSchema.ofEntity(EntityWithTopLevelId.class);
        assertThat(idSchema.flattenFields()).hasSize(1);
        assertThat(idSchema.flattenFieldNames()).containsExactly("id");
        assertThat(idSchema.flatten(new TopLevelId("zzz"))).containsExactly(entry("id", "zzz"));
    }

    @Test
    public void fail_on_non_static_inner_class_as_id_type() {
        assertThatIllegalArgumentException().isThrownBy(() -> EntitySchema.of(BadClassNonStaticId.class));
        assertThatIllegalArgumentException().isThrownBy(() -> EntityIdSchema.ofEntity(BadClassNonStaticId.class));
        assertThatIllegalArgumentException().isThrownBy(() -> EntityIdSchema.of(BadClassNonStaticId.Id.class));
    }

    @Test
    public void fail_on_non_static_inner_class_as_member_type() {
        assertThatIllegalArgumentException().isThrownBy(() -> EntityIdSchema.ofEntity(BadClassNonStaticMember.class));
        assertThatIllegalArgumentException().isThrownBy(() -> EntityIdSchema.of(BadClassNonStaticMember.Id.class));
        assertThatIllegalArgumentException().isThrownBy(() -> EntitySchema.of(BadClassNonStaticMember.class));
    }

    @Test
    public void allowed_id_filed_types() {
        EntitySchema<AllowedIdFieldTypeEntity> schema = EntitySchema.of(AllowedIdFieldTypeEntity.class);
        assertThat(schema.flattenId().stream().map(Schema.JavaField::getName))
                .containsExactlyInAnyOrder("id_boolValue", "id_intValue", "id_stringValue", "id_tsValue", "id_enumValue");

        EntityIdSchema<AllowedIdFieldTypeEntity.Id> idSchema = EntityIdSchema.of(AllowedIdFieldTypeEntity.Id.class);
        assertThat(idSchema.flattenFieldNames())
                .containsExactlyInAnyOrder("id_boolValue", "id_intValue", "id_stringValue", "id_tsValue", "id_enumValue");
    }

    @Test
    public void fail_on_not_allowed_id_filed_types() {
        assertThatIllegalArgumentException().isThrownBy(() -> EntityIdSchema.ofEntity(NotAllowedIdFieldTypeEntity.class));
        assertThatIllegalArgumentException().isThrownBy(() -> EntityIdSchema.of(NotAllowedIdFieldTypeEntity.Id.class));

        EntitySchema<NotAllowedIdFieldTypeEntity> schema = EntitySchema.of(NotAllowedIdFieldTypeEntity.class);
        assertThatIllegalArgumentException().isThrownBy(schema::getIdSchema);
    }

    @Test
    public void flatten_only_id_fields_of_entities() {
        EntityWithSingleFieldComposite entity = new EntityWithSingleFieldComposite(
                new EntityWithSingleFieldComposite.Id("ze_id"),
                new SingleFieldComposite("ze_composite"),
                42L
        );

        EntitySchema<EntityWithSingleFieldComposite> schema = EntitySchema.of(EntityWithSingleFieldComposite.class);
        assertThat(schema.flattenFieldNames())
                .containsExactlyInAnyOrder("id", "payload_singleField", "dummy");

        assertThat(schema.flatten(entity))
                .containsOnly(
                        entry("id", entity.id.value),
                        entry("payload_singleField", entity.payload.singleField),
                        entry("dummy", entity.dummy)
                );

        EntityWithSingleFieldComposite deserialized = schema.newInstance(Map.of(
                "id", entity.id.value,
                "payload_singleField", entity.payload.singleField,
                "dummy", entity.dummy));
        assertThat(deserialized).isEqualTo(entity);
    }

    @Test
    public void flatten_only_id_fields_of_entity_views() {
        ViewWithSingleFieldComposite view = new ViewWithSingleFieldComposite(
                new EntityWithSingleFieldComposite.Id("ze_id"),
                new SingleFieldComposite("ze_composite")
        );

        ViewSchema<ViewWithSingleFieldComposite> schema = ViewSchema.of(ViewWithSingleFieldComposite.class);
        assertThat(schema.flattenFieldNames())
                .containsExactlyInAnyOrder("id", "payload_singleField");

        assertThat(schema.flatten(view))
                .containsOnly(
                        entry("id", view.id.value),
                        entry("payload_singleField", view.payload.singleField)
                );

        ViewWithSingleFieldComposite deserialized = schema.newInstance(Map.of(
                "id", view.id.value,
                "payload_singleField", view.payload.singleField));
        assertThat(deserialized).isEqualTo(view);
    }

    @Test
    public void flatten_only_id_fields_of_entities_simple_parent_child() {
        ParentEntity entity = new ParentEntity(
                new ParentEntity.Id("parent_id"),
                new ChildEntity(
                        new ChildEntity.Id("child_id"),
                        42L,
                        new ChildEntity.CompositePayload(
                                "Hello, world!",
                                100_500
                        )
                )
        );

        EntitySchema<ParentEntity> schema = EntitySchema.of(ParentEntity.class);
        assertThat(schema.flattenFieldNames())
                .containsExactlyInAnyOrder(
                        "id",
                        "child_id",
                        "child_payload",
                        "child_compositePayload_str",
                        "child_compositePayload_integer"
                );

        assertThat(schema.flatten(entity))
                .containsOnly(
                        entry("id", entity.id.value),
                        entry("child_id", entity.child.id.value),
                        entry("child_payload", entity.child.payload),
                        entry("child_compositePayload_str", entity.child.compositePayload.str),
                        entry("child_compositePayload_integer", entity.child.compositePayload.integer)
                );

        ParentEntity deserialized = schema.newInstance(Map.of(
                "id", entity.id.value,
                "child_id", entity.child.id.value,
                "child_payload", entity.child.payload,
                "child_compositePayload_str", entity.child.compositePayload.str,
                "child_compositePayload_integer", entity.child.compositePayload.integer));
        assertThat(deserialized).isEqualTo(entity);
    }

    @Test
    public void marshal_composite_field_as_object() {
        WithUnflattenableField entity = new WithUnflattenableField(
                new WithUnflattenableField.Id("id"),
                new WithUnflattenableField.Unflattenable("str", 42));

        EntitySchema<WithUnflattenableField> schema = EntitySchema.of(WithUnflattenableField.class);
        assertThat(schema.flattenFieldNames())
                .containsExactlyInAnyOrder("id", "unflattenable");
        assertThat(schema.flatten(entity))
                .containsOnly(
                        entry("id", entity.id.value),
                        entry("unflattenable", entity.unflattenable)
                );

        WithUnflattenableField deserialized = schema.newInstance(Map.of(
                "id", entity.id.value,
                "unflattenable", entity.unflattenable));
        assertThat(deserialized).isEqualTo(entity);
    }

    @Test
    public void path_for_column_with_annotation() {
        EntitySchema<WithComplexIdAndAnnotations> schema = EntitySchema.of(WithComplexIdAndAnnotations.class);

        List<Schema.JavaField> idFields = schema.getIdSchema().flattenFields();

        assertThat(idFields).hasSize(2);
        assertThat(idFields.get(0).getPath()).isEqualTo("id.field1");
        assertThat(idFields.get(1).getPath()).isEqualTo("id.field2");
    }

    @Test
    public void constructor_failure_NPE_is_propagated_from_schema_newInstance_as_is() {
        assertThatExceptionOfType(ConstructionException.class)
                .isThrownBy(() -> EntitySchema.of(WithNonNullAnnotations.class).newInstance(Map.of("id", "hello_world_id")))
                .satisfies(ex -> assertThat(ex).hasCauseExactlyInstanceOf(NullPointerException.class));
    }

    @Test
    public void constructor_failure_IAE_is_propagated_from_schema_newInstance_as_is() {
        assertThatExceptionOfType(ConstructionException.class)
                .isThrownBy(() -> EntitySchema.of(WithExplicitConstructorCheck.class)
                        .newInstance(Map.of(
                                "id", "hello_world_id",
                                "value", 42L
                        ))
                )
                .satisfies(ex -> {
                    assertThat(ex).hasCauseExactlyInstanceOf(IllegalArgumentException.class);
                    assertThat(ex.getCause()).hasMessage("Bad value: 42");
                });
    }

    private record SampleEntity(
            Id id,
            String name,
            Instant created,

            String description,
            byte[] lotsOfData,

            Set<String> tags
    ) implements RecordEntity<SampleEntity> {
        private record Id(String zone, long localId) implements Entity.Id<SampleEntity> {
        }
    }

    private record SampleEntity2(FlatId id) implements RecordEntity<SampleEntity2> {
        record FlatId(String value) implements Id<SampleEntity2> {
        }
    }

    private record SampleEntity3(IdWithOtherId id) implements RecordEntity<SampleEntity3> {
        record IdWithOtherId(SampleEntity2.FlatId entity2Id) implements Id<SampleEntity3> {
        }
    }

    private record SampleEntity4(IdWithOtherIdAndEmptySubobject id) implements RecordEntity<SampleEntity4> {
        record IdWithOtherIdAndEmptySubobject(
                SampleEntity2.FlatId entity2Id,
                EmptySubobject empty
        ) implements Id<SampleEntity4> {
            record EmptySubobject() {
            }
        }
    }

    private record BadClassNonStaticId(Id id) implements RecordEntity<BadClassNonStaticId> {
        @Value
        @SuppressWarnings("InnerClassMayBeStatic")
        class Id implements Entity.Id<BadClassNonStaticId> {
            String value;
        }
    }

    private record BadClassNonStaticMember(
            Id id,
            NonStaticMember badMember
    ) implements RecordEntity<BadClassNonStaticMember> {
        private record Id(String value) implements Entity.Id<BadClassNonStaticMember> {
        }

        @Value
        @SuppressWarnings("InnerClassMayBeStatic")
        class NonStaticMember {
            String uzhos;
        }
    }

    private record AllowedIdFieldTypeEntity(Id id) implements RecordEntity<AllowedIdFieldTypeEntity> {
        private record Id(
                Boolean boolValue,
                Integer intValue,
                String stringValue,
                Instant tsValue,
                Status enumValue
        ) implements Entity.Id<AllowedIdFieldTypeEntity> {
        }

        enum Status {
        }
    }

    private record NotAllowedIdFieldTypeEntity(Id id) implements RecordEntity<NotAllowedIdFieldTypeEntity> {
        private record Id(Float floatValue) implements Entity.Id<NotAllowedIdFieldTypeEntity> {
        }
    }

    private record SampleEntityView(
            Instant created,
            Set<String> tags,
            String name
    ) implements View {
    }

    private record EntityWithSingleFieldComposite(
            Id id,
            SingleFieldComposite payload,
            long dummy    
    ) implements RecordEntity<EntityWithSingleFieldComposite> {
        private record Id(String value) implements Entity.Id<EntityWithSingleFieldComposite> {
        }
    }

    private record SingleFieldComposite(String singleField) {
    }

    private record ViewWithSingleFieldComposite(
            EntityWithSingleFieldComposite.Id id,
            SingleFieldComposite payload
    ) implements View {
    }

    private record ParentEntity(Id id, ChildEntity child) implements RecordEntity<ParentEntity> {
        private record Id(String value) implements Entity.Id<ParentEntity> {
        }
    }

    private record ChildEntity(
            Id id,
            long payload,
            CompositePayload compositePayload
    ) implements RecordEntity<ChildEntity> {
        private record Id(String value) implements Entity.Id<ChildEntity> {
        }

        private record CompositePayload(String str, int integer) {
        }
    }

    private record WithUnflattenableField(
            Id id,

            @Column(flatten = false)
            Unflattenable unflattenable
    ) implements RecordEntity<WithUnflattenableField> {
        private record Unflattenable(String str, int integer) {
        }

        private record Id(String value) implements Entity.Id<WithUnflattenableField> {
        }
    }

    private record WithComplexIdAndAnnotations(Id id) implements RecordEntity<WithComplexIdAndAnnotations> {
        private record Id(
                @Column(name = "field_1")
                String field1,

                String field2
        ) implements Entity.Id<WithComplexIdAndAnnotations> {
        }
    }

    private record WithNonNullAnnotations(
            Id id,

            @NonNull
            Data data
    ) implements RecordEntity<WithNonNullAnnotations> {
        private interface Data {
        }

        private record Id(String value) implements Entity.Id<WithNonNullAnnotations> {
        }
    }

    private record WithExplicitConstructorCheck(
            Id id,
            long value
    ) implements RecordEntity<WithExplicitConstructorCheck> {
        public WithExplicitConstructorCheck {
            Preconditions.checkArgument(value != 42L, "Bad value: %s", value);
        }

        private record Id(String value) implements Entity.Id<WithExplicitConstructorCheck> {
        }
    }

    private record TopLevelId(String value) implements Entity.Id<EntityWithTopLevelId> {
    }

    private record EntityWithTopLevelId(TopLevelId id) implements RecordEntity<EntityWithTopLevelId> {
    }
}
