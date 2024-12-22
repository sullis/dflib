package org.dflib.iceberg;

import java.util.HashMap;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.dflib.DataFrame;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test Iceberg tables
 */
public class IcebergLoaderTest {
    private InMemoryCatalog catalog;
    private Namespace namespace;
    private TableIdentifier tableIdentifier;
    private Schema schema;
    private Table table;

    @BeforeEach
    public void beforeEach() {
        namespace = Namespace.of("aaa.bbb");
        tableIdentifier = TableIdentifier.of(namespace, "ttt");
        schema = new Schema(
            Types.NestedField.of(0, true, "zzzInteger", Types.IntegerType.get()),
            Types.NestedField.of(1, true, "zzzBoolean", Types.BooleanType.get()),
            Types.NestedField.of(2, true, "zzzString", Types.StringType.get())
        );
        catalog = new InMemoryCatalog();
        catalog.initialize("catalogName", new HashMap<>());
        catalog.createNamespace(namespace);
        table = catalog.createTable(tableIdentifier, schema);
    }

    @Test
    public void readIcebergTable() {
        // todo
        table.refresh();
        DataFrame df = new IcebergLoader().load(tableIdentifier);
    }

}

