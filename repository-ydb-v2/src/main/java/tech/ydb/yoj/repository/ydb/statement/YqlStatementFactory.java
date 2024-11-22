package tech.ydb.yoj.repository.ydb.statement;

import tech.ydb.yoj.databind.expression.FilterExpression;
import tech.ydb.yoj.databind.expression.OrderExpression;
import tech.ydb.yoj.databind.schema.ObjectSchema;
import tech.ydb.yoj.repository.db.Entity;
import tech.ydb.yoj.repository.db.EntityDescriptor;
import tech.ydb.yoj.repository.db.EntityIdSchema;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.Range;
import tech.ydb.yoj.repository.db.Table;
import tech.ydb.yoj.repository.db.ViewSchema;
import tech.ydb.yoj.repository.ydb.yql.YqlStatementPart;

import java.util.Collection;
import java.util.List;

public interface YqlStatementFactory<ENTITY extends Entity<ENTITY>> {
    <PARAMS> Statement<PARAMS, ENTITY> insert(EntityDescriptor<ENTITY> descriptor);

    <PARAMS> Statement<PARAMS, ENTITY> save(EntityDescriptor<ENTITY> descriptor);

    <ID extends Entity.Id<ENTITY>> Statement<UpdateModel.ById<ID>, ?> update(
            EntityDescriptor<ENTITY> descriptor,
            UpdateModel.ById<ID> model
    );

    <PARAMS> Statement<PARAMS, ENTITY> delete(EntityDescriptor<ENTITY> descriptor);

    <PARAMS> Statement<PARAMS, ENTITY> deleteAll(EntityDescriptor<ENTITY> descriptor);

    <PARAMS> Statement<PARAMS, ENTITY> findAll(EntityDescriptor<ENTITY> descriptor);

    <PARAMS, VIEW extends Table.View> Statement<PARAMS, VIEW> findAll(EntityDescriptor<ENTITY> descriptor, Class<VIEW> viewType);

    Statement<Entity.Id<ENTITY>, ENTITY> find(EntityDescriptor<ENTITY> descriptor);

    <PARAMS, VIEW extends Table.View> Statement<PARAMS, VIEW> find(EntityDescriptor<ENTITY> descriptor, Class<VIEW> viewType);

    Statement<Collection<? extends YqlStatementPart<?>>, ENTITY> find(
            EntityDescriptor<ENTITY> descriptor,
            Collection<? extends YqlStatementPart<?>> parts
    );

    <VIEW extends Table.View> Statement<Collection<? extends YqlStatementPart<?>>, VIEW> find(
            EntityDescriptor<ENTITY> descriptor,
            Class<VIEW> viewType,
            boolean distinct,
            Collection<? extends YqlStatementPart<?>> parts
    );

    <ID extends Entity.Id<ENTITY>> Statement<Range<ID>, ENTITY> findRange(EntityDescriptor<ENTITY> descriptor, Range<ID> range);

    <VIEW extends Table.View, ID extends Entity.Id<ENTITY>> Statement<Range<ID>, VIEW> findRange(
            EntityDescriptor<ENTITY> descriptor,
            Class<VIEW> viewType,
            Range<ID> range
    );

    <PARAMS> Statement<PARAMS, ENTITY> findIn(
            EntityDescriptor<ENTITY> descriptor,
            Iterable<? extends Entity.Id<ENTITY>> ids,
            FilterExpression<ENTITY> filter,
            OrderExpression<ENTITY> orderBy,
            Integer limit
    );

    <PARAMS, VIEW extends Table.View> Statement<PARAMS, VIEW> findIn(
            EntityDescriptor<ENTITY> descriptor,
            Class<VIEW> viewType,
            Iterable<? extends Entity.Id<ENTITY>> ids,
            FilterExpression<ENTITY> filter,
            OrderExpression<ENTITY> orderBy,
            Integer limit
    );

    <PARAMS, K> Statement<PARAMS, ENTITY> findIn(
            EntityDescriptor<ENTITY> descriptor,
            String indexName,
            Iterable<K> keys,
            FilterExpression<ENTITY> filter,
            OrderExpression<ENTITY> orderBy,
            Integer limit
    );

    <PARAMS, VIEW extends Table.View, K> Statement<PARAMS, VIEW> findIn(
            EntityDescriptor<ENTITY> descriptor,
            Class<VIEW> viewType,
            String indexName,
            Iterable<K> keys,
            FilterExpression<ENTITY> filter,
            OrderExpression<ENTITY> orderBy,
            Integer limit
    );

    Statement<Collection<? extends YqlStatementPart<?>>, Count> count(
            EntityDescriptor<ENTITY> descriptor,
            List<YqlStatementPart<?>> partsList
    );

    <ID extends Entity.Id<ENTITY>> Statement<Collection<? extends YqlStatementPart<?>>, ID> findIds(
            EntityDescriptor<ENTITY> descriptor,
            Collection<? extends YqlStatementPart<?>> parts
    );

    <ID extends Entity.Id<ENTITY>> Statement<Range<ID>, ID> findIds(
            EntityDescriptor<ENTITY> descriptor,
            Range<ID> range
    );

    <PARAMS, ID extends Entity.Id<ENTITY>> Statement<PARAMS, ID> findIdsIn(
            EntityDescriptor<ENTITY> descriptor,
            Iterable<ID> ids,
            FilterExpression<ENTITY> filter,
            OrderExpression<ENTITY> orderBy,
            Integer limit
    );

    class Type<ENTITY extends Entity<ENTITY>> implements YqlStatementFactory<ENTITY> {
        @Override
        public <PARAMS> Statement<PARAMS, ENTITY> findAll(EntityDescriptor<ENTITY> descriptor) {
            return YqlStatement.findAll(descriptor.clazz());
        }

        @Override
        public <PARAMS, VIEW extends Table.View> Statement<PARAMS, VIEW> findAll(EntityDescriptor<ENTITY> descriptor, Class<VIEW> viewType) {
            return YqlStatement.findAll(descriptor.clazz(), viewType);
        }

        @Override
        public <PARAMS> Statement<PARAMS, ENTITY> deleteAll(EntityDescriptor<ENTITY> descriptor) {
            return YqlStatement.deleteAll(descriptor.clazz());
        }

        @Override
        public Statement<Entity.Id<ENTITY>, ENTITY> find(EntityDescriptor<ENTITY> descriptor) {
            return YqlStatement.find(descriptor.clazz());
        }

        @Override
        public <PARAMS, VIEW extends Table.View> Statement<PARAMS, VIEW> find(EntityDescriptor<ENTITY> descriptor, Class<VIEW> viewType) {
            return YqlStatement.find(descriptor.clazz(), viewType);
        }

        @Override
        public Statement<Collection<? extends YqlStatementPart<?>>, ENTITY> find(
                EntityDescriptor<ENTITY> descriptor,
                Collection<? extends YqlStatementPart<?>> parts
        ) {
            return YqlStatement.find(descriptor.clazz(), parts);
        }

        @Override
        public <VIEW extends Table.View> Statement<Collection<? extends YqlStatementPart<?>>, VIEW> find(
                EntityDescriptor<ENTITY> descriptor,
                Class<VIEW> viewType,
                boolean distinct,
                Collection<? extends YqlStatementPart<?>> parts
        ) {
            return YqlStatement.find(descriptor.clazz(), viewType, distinct, parts);
        }

        @Override
        public <PARAMS> Statement<PARAMS, ENTITY> insert(EntityDescriptor<ENTITY> descriptor) {
            return YqlStatement.insert(descriptor.clazz());
        }

        @Override
        public <PARAMS> Statement<PARAMS, ENTITY> save(EntityDescriptor<ENTITY> descriptor) {
            return YqlStatement.save(descriptor.clazz());
        }

        @Override
        public <ID extends Entity.Id<ENTITY>> Statement<Range<ID>, ENTITY> findRange(EntityDescriptor<ENTITY> descriptor, Range<ID> range) {
            return YqlStatement.findRange(descriptor.clazz(), range);
        }

        @Override
        public <VIEW extends Table.View, ID extends Entity.Id<ENTITY>> Statement<Range<ID>, VIEW> findRange(
                EntityDescriptor<ENTITY> descriptor,
                Class<VIEW> viewType,
                Range<ID> range
        ) {
            return YqlStatement.findRange(descriptor.clazz(), viewType, range);
        }

        @Override
        public <PARAMS> Statement<PARAMS, ENTITY> findIn(
                EntityDescriptor<ENTITY> descriptor,
                Iterable<? extends Entity.Id<ENTITY>> ids,
                FilterExpression<ENTITY> filter,
                OrderExpression<ENTITY> orderBy,
                Integer limit
        ) {
            return YqlStatement.findIn(descriptor.clazz(), ids, filter, orderBy, limit);
        }

        @Override
        public <PARAMS, VIEW extends Table.View> Statement<PARAMS, VIEW> findIn(
                EntityDescriptor<ENTITY> descriptor,
                Class<VIEW> viewType,
                Iterable<? extends Entity.Id<ENTITY>> ids,
                FilterExpression<ENTITY> filter,
                OrderExpression<ENTITY> orderBy,
                Integer limit
        ) {
            return YqlStatement.findIn(descriptor.clazz(), viewType, ids, filter, orderBy, limit);
        }

        @Override
        public <PARAMS, K> Statement<PARAMS, ENTITY> findIn(
                EntityDescriptor<ENTITY> descriptor,
                String indexName, Iterable<K> keys,
                FilterExpression<ENTITY> filter,
                OrderExpression<ENTITY> orderBy,
                Integer limit
        ) {
            return YqlStatement.findIn(descriptor.clazz(), indexName, keys, filter, orderBy, limit);
        }

        @Override
        public <PARAMS, VIEW extends Table.View, K> Statement<PARAMS, VIEW> findIn(
                EntityDescriptor<ENTITY> descriptor,
                Class<VIEW> viewType,
                String indexName,
                Iterable<K> keys,
                FilterExpression<ENTITY> filter,
                OrderExpression<ENTITY> orderBy,
                Integer limit
        ) {
            return YqlStatement.findIn(descriptor.clazz(), viewType, indexName, keys, filter, orderBy, limit);
        }

        @Override
        public <PARAMS> Statement<PARAMS, ENTITY> delete(EntityDescriptor<ENTITY> descriptor) {
            return YqlStatement.delete(descriptor.clazz());
        }

        @Override
        public Statement<Collection<? extends YqlStatementPart<?>>, Count> count(
                EntityDescriptor<ENTITY> descriptor,
                List<YqlStatementPart<?>> partsList
        ) {
            return YqlStatement.count(descriptor.clazz(), partsList);
        }

        @Override
        public <ID extends Entity.Id<ENTITY>> Statement<Collection<? extends YqlStatementPart<?>>, ID> findIds(
                EntityDescriptor<ENTITY> descriptor,
                Collection<? extends YqlStatementPart<?>> parts
        ) {
            return YqlStatement.findIds(descriptor.clazz(), parts);
        }

        @Override
        public <ID extends Entity.Id<ENTITY>> Statement<Range<ID>, ID> findIds(EntityDescriptor<ENTITY> descriptor, Range<ID> range) {
            return YqlStatement.findIds(descriptor.clazz(), range);
        }

        @Override
        public <PARAMS, ID extends Entity.Id<ENTITY>> Statement<PARAMS, ID> findIdsIn(
                EntityDescriptor<ENTITY> descriptor,
                Iterable<ID> ids,
                FilterExpression<ENTITY> filter,
                OrderExpression<ENTITY> orderBy,
                Integer limit
        ) {
            return YqlStatement.findIdsIn(descriptor.clazz(), ids, filter, orderBy, limit);
        }

        @Override
        public <ID extends Entity.Id<ENTITY>> Statement<UpdateModel.ById<ID>, ?> update(
                EntityDescriptor<ENTITY> descriptor,
                UpdateModel.ById<ID> model
        ) {
            return YqlStatement.update(descriptor.clazz(), model);
        }
    }

    class Descriptor<ENTITY extends Entity<ENTITY>> implements YqlStatementFactory<ENTITY> {
        @Override
        public <PARAMS> Statement<PARAMS, ENTITY> insert(EntityDescriptor<ENTITY> descriptor) {
            EntitySchema<ENTITY> schema = EntitySchema.of(descriptor.clazz());
            return new InsertYqlStatement<>(descriptor.clazz(), descriptor.getTableName(schema));
        }

        @Override
        public <PARAMS> Statement<PARAMS, ENTITY> save(EntityDescriptor<ENTITY> descriptor) {
            EntitySchema<ENTITY> schema = EntitySchema.of(descriptor.clazz());
            return new UpsertYqlStatement<>(descriptor.clazz(), descriptor.getTableName(schema));
        }

        @Override
        public <ID extends Entity.Id<ENTITY>> Statement<Range<ID>, ENTITY> findRange(EntityDescriptor<ENTITY> descriptor, Range<ID> range) {
            EntitySchema<ENTITY> schema = EntitySchema.of(descriptor.clazz());
            return new FindRangeStatement<>(schema, schema, range, descriptor.getTableName(schema));
        }

        @Override
        public <VIEW extends Table.View, ID extends Entity.Id<ENTITY>> Statement<Range<ID>, VIEW> findRange(
                EntityDescriptor<ENTITY> descriptor,
                Class<VIEW> viewType,
                Range<ID> range
        ) {
            EntitySchema<ENTITY> schema = EntitySchema.of(descriptor.clazz());
            return new FindRangeStatement<>(schema, ViewSchema.of(viewType), range, descriptor.getTableName(schema));
        }

        @Override
        public <PARAMS> Statement<PARAMS, ENTITY> findIn(
                EntityDescriptor<ENTITY> descriptor,
                Iterable<? extends Entity.Id<ENTITY>> ids,
                FilterExpression<ENTITY> filter,
                OrderExpression<ENTITY> orderBy,
                Integer limit
        ) {
            EntitySchema<ENTITY> schema = EntitySchema.of(descriptor.clazz());
            return new FindInStatement<>(schema, schema, ids, filter, orderBy, limit, descriptor.getTableName(schema));
        }

        @Override
        public <PARAMS, VIEW extends Table.View> Statement<PARAMS, VIEW> findIn(
                EntityDescriptor<ENTITY> descriptor,
                Class<VIEW> viewType,
                Iterable<? extends Entity.Id<ENTITY>> ids,
                FilterExpression<ENTITY> filter,
                OrderExpression<ENTITY> orderBy,
                Integer limit
        ) {
            EntitySchema<ENTITY> schema = EntitySchema.of(descriptor.clazz());
            return new FindInStatement<>(schema, ViewSchema.of(viewType), ids, filter, orderBy, limit, descriptor.getTableName(schema));
        }

        @Override
        public <PARAMS, K> Statement<PARAMS, ENTITY> findIn(
                EntityDescriptor<ENTITY> descriptor,
                String indexName, Iterable<K> keys,
                FilterExpression<ENTITY> filter,
                OrderExpression<ENTITY> orderBy,
                Integer limit
        ) {
            EntitySchema<ENTITY> schema = EntitySchema.of(descriptor.clazz());
            return new FindInStatement<>(schema, schema, indexName, keys, filter, orderBy, limit, descriptor.getTableName(schema));
        }

        @Override
        public <PARAMS, VIEW extends Table.View, K> Statement<PARAMS, VIEW> findIn(
                EntityDescriptor<ENTITY> descriptor,
                Class<VIEW> viewType,
                String indexName,
                Iterable<K> keys,
                FilterExpression<ENTITY> filter,
                OrderExpression<ENTITY> orderBy,
                Integer limit
        ) {
            EntitySchema<ENTITY> schema = EntitySchema.of(descriptor.clazz());
            return new FindInStatement<>(schema, ViewSchema.of(viewType), indexName, keys, filter, orderBy, limit, descriptor.getTableName(schema));
        }

        @Override
        public <PARAMS> Statement<PARAMS, ENTITY> findAll(EntityDescriptor<ENTITY> descriptor) {
            EntitySchema<ENTITY> schema = EntitySchema.of(descriptor.clazz());
            return new FindAllYqlStatement<>(schema, schema, descriptor.getTableName(schema));
        }

        @Override
        public <PARAMS, VIEW extends Table.View> Statement<PARAMS, VIEW> findAll(EntityDescriptor<ENTITY> descriptor, Class<VIEW> viewType) {
            EntitySchema<ENTITY> schema = EntitySchema.of(descriptor.clazz());
            return new FindAllYqlStatement<>(schema, ViewSchema.of(viewType), descriptor.getTableName(schema));
        }

        @Override
        public <PARAMS> Statement<PARAMS, ENTITY> deleteAll(EntityDescriptor<ENTITY> descriptor) {
            EntitySchema<ENTITY> schema = EntitySchema.of(descriptor.clazz());
            return new DeleteAllStatement<>(schema, descriptor.getTableName(schema));
        }

        @Override
        public <PARAMS> Statement<PARAMS, ENTITY> delete(EntityDescriptor<ENTITY> descriptor) {
            EntitySchema<ENTITY> schema = EntitySchema.of(descriptor.clazz());
            return new DeleteByIdStatement<>(descriptor.clazz(), descriptor.getTableName(schema));
        }

        @Override
        public Statement<Collection<? extends YqlStatementPart<?>>, Count> count(
                EntityDescriptor<ENTITY> descriptor,
                List<YqlStatementPart<?>> parts
        ) {
            EntitySchema<ENTITY> schema = EntitySchema.of(descriptor.clazz());
            return new CountAllStatement<>(schema, ObjectSchema.of(Count.class), parts, YqlStatement::predicateFrom, descriptor.getTableName(schema));
        }

        @Override
        public <ID extends Entity.Id<ENTITY>> Statement<Collection<? extends YqlStatementPart<?>>, ID> findIds(
                EntityDescriptor<ENTITY> descriptor,
                Collection<? extends YqlStatementPart<?>> parts
        ) {
            return YqlStatement.findIds(descriptor.clazz(), parts);
        }

        @Override
        public <ID extends Entity.Id<ENTITY>> Statement<Range<ID>, ID> findIds(EntityDescriptor<ENTITY> descriptor, Range<ID> range) {
            EntitySchema<ENTITY> schema = EntitySchema.of(descriptor.clazz());
            return new FindRangeStatement<>(schema, EntityIdSchema.ofEntity(descriptor.clazz()), range, descriptor.getTableName(schema));
        }

        @Override
        public <PARAMS, ID extends Entity.Id<ENTITY>> Statement<PARAMS, ID> findIdsIn(
                EntityDescriptor<ENTITY> descriptor,
                Iterable<ID> ids,
                FilterExpression<ENTITY> filter,
                OrderExpression<ENTITY> orderBy,
                Integer limit
        ) {
            EntitySchema<ENTITY> schema = EntitySchema.of(descriptor.clazz());
            return new FindInStatement<>(schema, EntityIdSchema.ofEntity(descriptor.clazz()), ids, filter, orderBy, limit, descriptor.getTableName(schema));
        }

        @Override
        public <ID extends Entity.Id<ENTITY>> Statement<UpdateModel.ById<ID>, ?> update(
                EntityDescriptor<ENTITY> descriptor,
                UpdateModel.ById<ID> model
        ) {
            EntitySchema<ENTITY> schema = EntitySchema.of(descriptor.clazz());
            return new UpdateByIdStatement<>(descriptor.clazz(), model, descriptor.getTableName(schema));
        }

        @Override
        public Statement<Entity.Id<ENTITY>, ENTITY> find(EntityDescriptor<ENTITY> descriptor) {
            EntitySchema<ENTITY> schema = EntitySchema.of(descriptor.clazz());
            return new FindYqlStatement<>(schema, schema, descriptor.getTableName(schema));
        }

        @Override
        public <PARAMS, VIEW extends Table.View> Statement<PARAMS, VIEW> find(EntityDescriptor<ENTITY> descriptor, Class<VIEW> viewType) {
            EntitySchema<ENTITY> schema = EntitySchema.of(descriptor.clazz());
            return new FindYqlStatement<>(schema, ViewSchema.of(viewType), descriptor.getTableName(schema));
        }

        @Override
        public Statement<Collection<? extends YqlStatementPart<?>>, ENTITY> find(
                EntityDescriptor<ENTITY> descriptor,
                Collection<? extends YqlStatementPart<?>> parts
        ) {
            EntitySchema<ENTITY> schema = EntitySchema.of(descriptor.clazz());
            return YqlStatement.find(schema, schema, false, parts, descriptor.getTableName(schema));
        }

        @Override
        public <VIEW extends Table.View> Statement<Collection<? extends YqlStatementPart<?>>, VIEW> find(
                EntityDescriptor<ENTITY> descriptor,
                Class<VIEW> viewType,
                boolean distinct,
                Collection<? extends YqlStatementPart<?>> parts
        ) {
            EntitySchema<ENTITY> schema = EntitySchema.of(descriptor.clazz());
            return YqlStatement.find(schema, ViewSchema.of(viewType), distinct, parts, descriptor.getTableName(schema));
        }
    }
}
