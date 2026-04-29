package tech.ydb.yoj.repository.db;

import org.junit.Test;
import tech.ydb.yoj.repository.db.CustomTable.CustomTableException;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;

public class CustomizedTableTest {
    private record MyEntity(Id id) implements RecordEntity<MyEntity> {
        private record Id(String value) implements Entity.Id<MyEntity> {
        }
    }

    @Test
    public void overriddenSave() {
        var id = new MyEntity.Id("id");
        var entity = new MyEntity(id);

        var table = new CustomTable<MyEntity>();
        assertThatExceptionOfType(CustomTableException.class).isThrownBy(() -> table.save(entity));
        assertThatExceptionOfType(CustomTableException.class).isThrownBy(() -> table.saveOrUpdate(entity));
        assertThatExceptionOfType(CustomTableException.class).isThrownBy(() -> table.saveNewOrThrow(entity, IllegalArgumentException::new));
        assertThatExceptionOfType(CustomTableException.class).isThrownBy(() -> table.generateAndSaveNew(() -> entity));
    }

    @Test
    public void overriddenDelete() {
        var id = new MyEntity.Id("id");

        var table = new CustomTable<MyEntity>();
        assertThatExceptionOfType(CustomTableException.class).isThrownBy(() -> table.delete(id));
        assertThatNoException().isThrownBy(() -> table.deleteIfExists(id)); // because Table.find() returns `null`
        assertThatExceptionOfType(CustomTableException.class).isThrownBy(() -> table.deleteAll(Set.of(id)));
    }
}
