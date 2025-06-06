package tech.ydb.yoj.repository.ydb.list;

import org.junit.ClassRule;
import org.junit.Test;
import tech.ydb.yoj.databind.expression.FilterExpression;
import tech.ydb.yoj.repository.db.EntitySchema;
import tech.ydb.yoj.repository.db.Repository;
import tech.ydb.yoj.repository.db.list.ListRequest;
import tech.ydb.yoj.repository.db.list.ListResult;
import tech.ydb.yoj.repository.test.ListingTest;
import tech.ydb.yoj.repository.test.sample.model.Complex;
import tech.ydb.yoj.repository.ydb.TestYdbRepository;
import tech.ydb.yoj.repository.ydb.YdbEnvAndTransportRule;
import tech.ydb.yoj.repository.ydb.yql.YqlListingQuery;
import tech.ydb.yoj.repository.ydb.yql.YqlPredicate;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.ydb.yoj.repository.db.EntityExpressions.newFilterBuilder;

public class YdbListingIntegrationTest extends ListingTest {
    @ClassRule
    public static final YdbEnvAndTransportRule ydbEnvAndTransport = new YdbEnvAndTransportRule();

    @Override
    protected Repository createTestRepository() {
        return new TestYdbRepository(ydbEnvAndTransport.getYdbConfig(), ydbEnvAndTransport.getGrpcTransport());
    }

    @Test
    public void idPredicatesAreProperlyOrdered() {
        Complex c1 = new Complex(new Complex.Id(999_999, 15L, "ZZZ", Complex.Status.OK));
        Complex c2 = new Complex(new Complex.Id(999_999, 15L, "UUU", Complex.Status.OK));
        Complex c3 = new Complex(new Complex.Id(999_999, 15L, "KKK", Complex.Status.OK));
        Complex c4 = new Complex(new Complex.Id(999_000, 15L, "AAA", Complex.Status.OK));
        db.tx(() -> db.complexes().insert(c1, c2, c3, c4));

        FilterExpression<Complex> filter = newFilterBuilder(Complex.class).where("id.b").eq(15L).and("id.a").eq(999_999).build();
        ListRequest<Complex> listRequest = ListRequest.builder(Complex.class).pageSize(3).filter(filter).build();

        YqlPredicate predicate = YqlListingQuery.toYqlPredicate(filter);
        assertThat(predicate.toYql(EntitySchema.of(Complex.class))).isEqualTo("(`id_a` = ?) AND (`id_b` = ?)");

        db.tx(() -> {
            ListResult<Complex> page = db.complexes().list(listRequest);
            assertThat(page).containsExactly(c3, c2, c1);
            assertThat(page.isLastPage()).isTrue();
        });
    }
}
