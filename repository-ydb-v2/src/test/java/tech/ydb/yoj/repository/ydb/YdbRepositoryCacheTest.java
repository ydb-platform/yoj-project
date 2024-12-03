package tech.ydb.yoj.repository.ydb;

import com.google.common.collect.ImmutableSet;
import lombok.NonNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import tech.ydb.core.Result;
import tech.ydb.proto.ValueProtos;
import tech.ydb.proto.table.YdbTable;
import tech.ydb.table.Session;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.StructType;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.Range;
import tech.ydb.yoj.repository.db.exception.EntityAlreadyExistsException;
import tech.ydb.yoj.repository.test.sample.TestEntityOperations;
import tech.ydb.yoj.repository.test.sample.model.Complex;
import tech.ydb.yoj.repository.test.sample.model.Complex.Id;
import tech.ydb.yoj.repository.ydb.client.SessionManager;
import tech.ydb.yoj.repository.ydb.client.YdbConverter;
import tech.ydb.yoj.repository.ydb.statement.FindYqlStatement;
import tech.ydb.yoj.repository.ydb.statement.MultipleVarsYqlStatement;
import tech.ydb.yoj.repository.ydb.statement.UpsertYqlStatement;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class YdbRepositoryCacheTest {
    @Mock
    private Session session;
    @Mock
    private SessionManager sessionManager;
    @Mock
    private TestYdbRepository testYdbRepository;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        when(testYdbRepository.getSessionManager()).thenReturn(sessionManager);
        when(sessionManager.getSession()).thenReturn(session);
    }

    @Test
    public void readFromCache() {
        Id id = new Id(1, 2L, "c", Complex.Status.OK);

        TestEntityOperations ops = new TestYdbRepository.TestYdbRepositoryTransaction(testYdbRepository);

        when(session.executeDataQuery(any(), any(), any(), any()))
                .thenReturn(convertEntity(List.of(new Complex(id))));

        ops.complexes().find(id);
        ops.complexes().find(id);
        ops.complexes().find(id);

        verify(session, times(1)).executeDataQuery(any(), any(), refEq(convertId(id)), any());
    }

    @Test
    public void readFromCacheNonExisted() {
        Complex.Id id = new Complex.Id(1, 2L, "c", Complex.Status.OK);

        TestEntityOperations ops = new TestYdbRepository.TestYdbRepositoryTransaction(testYdbRepository);

        when(session.executeDataQuery(any(), any(), any(), any())).thenReturn(convertEntity(emptyList()));

        ops.complexes().find(id);
        ops.complexes().find(id);

        verify(session, times(1)).executeDataQuery(any(), any(), refEq(convertId(id)), any());
    }

    @Test
    public void findRangeAndPutInCache() {
        List<Complex> results = createComplexesList();

        TestEntityOperations ops = new TestYdbRepository.TestYdbRepositoryTransaction(testYdbRepository);

        when(session.executeDataQuery(any(), any(), any(), any())).thenReturn(convertEntity(results));

        ops.complexes().find(Range.create(new Complex.Id(1, null, null, null)));
        for (Complex c : results) {
            ops.complexes().find(c.getId());
        }

        verify(session, times(1)).executeDataQuery(any(), any(), any(), any());
    }

    @Test
    public void findInAndPutInCache() {
        List<Complex> results = createComplexesList();

        TestEntityOperations ops = new TestYdbRepository.TestYdbRepositoryTransaction(testYdbRepository);

        when(session.executeDataQuery(any(), any(), any(), any())).thenReturn(convertEntity(results));

        ImmutableSet<Id> ids = ImmutableSet.of(new Id(1, 0L, null, null), new Id(1, 1L, null, null));
        ops.complexes().find(ids);
        for (Complex c : results) {
            ops.complexes().find(c.getId());
        }

        verify(session, times(1)).executeDataQuery(any(), any(), any(), any());
    }

    @Test
    public void findInAndPutInCacheAndReadFromCacheAll() {
        List<Complex> results = createComplexesList();

        TestEntityOperations ops = new TestYdbRepository.TestYdbRepositoryTransaction(testYdbRepository);

        when(session.executeDataQuery(any(), any(), any(), any())).thenReturn(convertEntity(results));

        var ids = Set.of(
                new Id(1, 0L, "c", Complex.Status.OK),
                new Id(1, 1L, "c", Complex.Status.OK)
        );
        ops.complexes().find(ids);
        ops.complexes().find(ids);

        verify(session, times(1)).executeDataQuery(any(), any(), any(), any());
    }

    @Test
    public void findInAndPutInCacheAndReadFromCachePartially() {
        /* Test that ids which was not found in cache will be fetched from db */
        List<Complex> results = createComplexesList();
        var firstItem = results.get(0);
        var secondItem = results.get(1);

        TestEntityOperations ops = new TestYdbRepository.TestYdbRepositoryTransaction(testYdbRepository);

        when(session.executeDataQuery(any(), any(), any(), any())).thenReturn(convertEntity(List.of(firstItem)));
        ops.complexes().find(firstItem.getId());
        // first should be in cache now

        // next execute return only second but we should get both (first from cache)
        clearInvocations();
        when(session.executeDataQuery(any(), any(), any(), any())).thenReturn(convertEntity(List.of(secondItem)));

        ImmutableSet<Id> ids = ImmutableSet.of(firstItem.getId(), secondItem.getId());
        var fetched = ops.complexes().find(ids);

        verify(session, times(2)).executeDataQuery(any(), any(), any(), any());
        Assert.assertEquals(ImmutableSet.of(firstItem, secondItem), new HashSet<>(fetched));
    }

    @Test
    public void findInAndPutInCacheAndReadFromCacheIncompleteIds() {
        /*
         * Test that find(Set<>) with mixed (complete & incomplete ids) returns:
         *   * entries matched by complete ids that was already cached AS IS FROM CACHE
         *   * entries matched by incomplete ids that was not cached AS IS FROM DB
         *   * entries matched by incomplete ids that was already cached AS IS FROM CACHE
         *  */
        var first = new Complex(new Complex.Id(1, 1L, "", Complex.Status.OK), "actual");
        var firstOutdated = new Complex(new Complex.Id(1, 1L, "", Complex.Status.OK), "outdated");
        var second = new Complex(new Complex.Id(1, 2L, "", Complex.Status.OK), "actual");
        var third = new Complex(new Complex.Id(2, 1L, "", Complex.Status.OK), "actual");

        TestEntityOperations ops = new TestYdbRepository.TestYdbRepositoryTransaction(testYdbRepository);
        when(session.executeDataQuery(any(), any(), any(), any())).thenReturn(convertEntity(List.of(first)));
        ops.complexes().find(first.getId());
        clearInvocations();
        when(session.executeDataQuery(any(), any(), any(), any())).thenReturn(convertEntity(List.of(third)));
        ops.complexes().find(third.getId());
        // first with actual value and third should be in cache now

        clearInvocations();
        // we should get first from cache (actual) and second from db
        when(session.executeDataQuery(any(), any(), any(), any())).thenReturn(convertEntity(List.of(firstOutdated, second)));
        var fetched = ops.complexes().find(ImmutableSet.of(
                new Complex.Id(1, null, null, null),
                third.getId()
        ));
        verify(session, times(3)).executeDataQuery(any(), any(), any(), any());
        Assert.assertEquals(ImmutableSet.of(first, second, third), new HashSet<>(fetched));
    }

    @Test
    public void findInDoesNotOverwriteCache() {
        var id = new Id(1, 1L, "c", Complex.Status.OK);
        var firstActual = new Complex(id, "actual");
        var firstOutdated = new Complex(id, "outdated");
        TestEntityOperations ops = new TestYdbRepository.TestYdbRepositoryTransaction(testYdbRepository);

        when(session.executeDataQuery(any(), any(), any(), any())).thenReturn(convertEntity(singletonList(firstActual)));
        var found = ops.complexes().find(id);
        Assert.assertEquals(found, firstActual);

        when(session.executeDataQuery(any(), any(), any(), any())).thenReturn(convertEntity(singletonList(firstOutdated)));
        ops.complexes().find(ImmutableSet.of(new Id(1, null, null, null)));
        found = ops.complexes().find(id);
        Assert.assertEquals(found, firstActual);

        verify(session, times(2)).executeDataQuery(any(), any(), any(), any());
    }

    @Test
    public void findInFillsCacheForFoundCompleteIds() {
        var existsInDbId = new Id(1, 1L, "c", Complex.Status.OK);
        var existsInDb = new Complex(existsInDbId);

        TestEntityOperations ops = new TestYdbRepository.TestYdbRepositoryTransaction(testYdbRepository);

        when(session.executeDataQuery(any(), any(), any(), any())).thenReturn(convertEntity(singletonList(existsInDb)));
        var found = ops.complexes().find(ImmutableSet.of(existsInDbId));
        Assert.assertEquals(found, singletonList(existsInDb));

        var foundInCache = ops.complexes().find(existsInDbId);
        Assert.assertEquals(existsInDb, foundInCache);

        verify(session, times(1)).executeDataQuery(any(), any(), any(), any());
    }

    @Test
    public void findInFillsCacheForFoundPartialIds() {
        var existsInDbId = new Id(1, 1L, "c", Complex.Status.OK);
        var existsInDb = new Complex(existsInDbId);

        TestEntityOperations ops = new TestYdbRepository.TestYdbRepositoryTransaction(testYdbRepository);

        when(session.executeDataQuery(any(), any(), any(), any())).thenReturn(convertEntity(singletonList(existsInDb)));
        var found = ops.complexes().find(ImmutableSet.of(new Id(1, null, null, null)));
        Assert.assertEquals(found, singletonList(existsInDb));

        var foundInCache = ops.complexes().find(existsInDbId);
        Assert.assertEquals(existsInDb, foundInCache);

        verify(session, times(1)).executeDataQuery(any(), any(), any(), any());
    }

    @Test
    public void findInFillsCacheForEmptyCompleteIds() {
        var existsInDbId = new Id(1, 1L, "c", Complex.Status.OK);
        var existsInDb = new Complex(existsInDbId);
        var notExistInDbId = new Id(2, 2L, "c", Complex.Status.OK);
        TestEntityOperations ops = new TestYdbRepository.TestYdbRepositoryTransaction(testYdbRepository);

        when(session.executeDataQuery(any(), any(), any(), any())).thenReturn(convertEntity(singletonList(existsInDb)));
        var found = ops.complexes().find(ImmutableSet.of(existsInDbId, notExistInDbId));
        Assert.assertEquals(found, singletonList(existsInDb));

        var foundInCache = ops.complexes().find(notExistInDbId);
        Assert.assertNull(foundInCache);

        verify(session, times(1)).executeDataQuery(any(), any(), any(), any());
    }

    @Test
    public void findAllAndPutInCache() {
        List<Complex> results = createComplexesList();

        TestEntityOperations ops = new TestYdbRepository.TestYdbRepositoryTransaction(testYdbRepository);

        when(session.executeDataQuery(any(), any(), any(), any())).thenReturn(convertEntity(results));

        ops.complexes().findAll();
        for (Complex c : results) {
            ops.complexes().find(c.getId());
        }

        verify(session, times(1)).executeDataQuery(any(), any(), any(), any());
    }

    @Test
    public void doNotSaveEntitiesIfTheSameValuesInCache() {
        Complex.Id id = new Complex.Id(1, 2L, "c", Complex.Status.OK);

        TestYdbRepository.TestYdbRepositoryTransaction ops =
                new TestYdbRepository.TestYdbRepositoryTransaction(testYdbRepository);

        when(session.executeDataQuery(any(), any(), any(), any()))
                .thenReturn(convertEntity(singletonList(new Complex(id))));

        ops.complexes().find(id);
        ops.complexes().save(new Complex(id));
        ops.commit();

        verify(session, times(1)).executeDataQuery(any(), any(), any(), any());
        verify(session, times(1)).executeDataQuery(any(), any(), refEq(convertId(id)), any());
    }

    @Test(expected = EntityAlreadyExistsException.class)
    public void throwEntityAlreadyExistsIfTheSameKeyInCache() {
        Complex.Id id = new Complex.Id(1, 2L, "c", Complex.Status.OK);

        TestYdbRepository.TestYdbRepositoryTransaction ops =
                new TestYdbRepository.TestYdbRepositoryTransaction(testYdbRepository);

        when(session.executeDataQuery(any(), any(), any(), any()))
                .thenReturn(convertEntity(singletonList(new Complex(id))));

        ops.complexes().find(id);
        ops.complexes().insert(new Complex(id));
        ops.commit();
    }

    private List<Complex> createComplexesList() {
        List<Complex> results = new ArrayList<>();
        for (long i = 0; i < 3; i++) {
            results.add(new Complex(new Complex.Id(1, i, "c", Complex.Status.OK)));
        }
        return results;
    }

    @NonNull
    private CompletableFuture<Result<DataQueryResult>> convertEntity(List<Complex> complexes) {
        ValueProtos.ResultSet.Builder builder = ValueProtos.ResultSet.newBuilder();
        complexes.stream()
                .map(complex -> new UpsertYqlStatement<>(EntitySchema.of(Complex.class)).toQueryParameters(complex))
                .map(map -> YdbConverter.convertToParams(map).values().get(MultipleVarsYqlStatement.listName))
                .peek(value -> {
                    if (builder.getColumnsCount() == 0) {
                        StructType itemType = (StructType) value.asList().getType().getItemType();
                        for (int i = 0; i < itemType.getMembersCount(); i++) {
                            builder.addColumns(ValueProtos.Column.newBuilder()
                                    .setName(itemType.getMemberName(i))
                                    .setType(itemType.getMemberType(i).toPb())
                                    .build());
                        }
                    }
                })
                .forEach(value -> {
                    StructType itemType = (StructType) value.asList().getType().getItemType();
                    ValueProtos.Value.Builder valueBuilder = ValueProtos.Value.newBuilder();
                    for (int i = 0; i < itemType.getMembersCount(); i++) {
                        valueBuilder.addItems(value.asList().get(0).asStuct().getMemberValue(i).toPb());
                    }
                    builder.addRows(valueBuilder.build());
                });
        var executeQueryResult = YdbTable.ExecuteQueryResult.newBuilder()
                .addResultSets(builder.build())
                .build();
        return CompletableFuture.completedFuture(Result.success(new DataQueryResult(executeQueryResult)));
    }

    private Params convertId(Id id) {
        var schema = EntitySchema.of(Complex.class);
        return YdbConverter.convertToParams(new FindYqlStatement<>(schema, schema).toQueryParameters(id));
    }
}
