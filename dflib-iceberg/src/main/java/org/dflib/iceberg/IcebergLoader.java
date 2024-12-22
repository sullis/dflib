package org.dflib.iceberg;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.dflib.DataFrame;
import org.dflib.Extractor;
import org.dflib.Index;
import org.dflib.iceberg.read.SchemaProjector;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergLoader {

    private SchemaProjector schemaProjector;
    private final List<ColConfigurator> colConfigurators;

    public IcebergLoader() {
        this.colConfigurators = new ArrayList<>();
    }

    /**
     * Configures the loader to only process the specified columns, and include them in the DataFrame in the specified
     * order.
     *
     * @return this loader instance
     */
    public IcebergLoader cols(String... columns) {
        this.schemaProjector = SchemaProjector.ofCols(columns);
        return this;
    }

    /**
     * @return this loader instance
     */
    public IcebergLoader cols(int... columns) {
        this.schemaProjector = SchemaProjector.ofCols(columns);
        return this;
    }

    /**
     * @return this loader instance
     */
    public IcebergLoader colsExcept(String... columns) {
        this.schemaProjector = SchemaProjector.ofColsExcept(columns);
        return this;
    }

    /**
     * @return this loader instance
     */
    public IcebergLoader colsExcept(int... columns) {
        this.schemaProjector = SchemaProjector.ofColsExcept(columns);
        return this;
    }

    /**
     * Configures a Parquet column to be loaded with value compaction. Should be used to save memory for low-cardinality
     * columns. Note that Parquet already does compaction on String columns by default, but some other column types
     * can take advantage of an explicit compaction.
     */
    public IcebergLoader compactCol(int column) {
        colConfigurators.add(ColConfigurator.objectCol(column, true));
        return this;
    }

    /**
     * Configures a Parquet column to be loaded with value compaction. Should be used to save memory for low-cardinality
     * columns. Note that Parquet already does compaction on String columns by default, but some other column types
     * can take advantage of an explicit compaction.
     */
    public IcebergLoader compactCol(String column) {
        colConfigurators.add(ColConfigurator.objectCol(column, true));
        return this;
    }

    public DataFrame load(File file) {
        return load(file.toPath());
    }

    public DataFrame load(String filePath) {
        return load(new File(filePath));
    }

    public DataFrame load(Path filePath) {
        // todo
        return null;
    }

    public DataFrame load(TableIdentifier tableIdentifier) {
        // todo
        return null;
    }

    private MessageType projectSchema(MessageType schema) {
        return schemaProjector != null
                ? schemaProjector.project(schema)
                : schema;
    }

    private Index createIndex(GroupType schema) {
        String[] labels = schema.getFields().stream().map(Type::getName).toArray(String[]::new);
        return Index.of(labels);
    }

    private Extractor<Object[], ?>[] extractors(Index index, GroupType schema) {

        Map<Integer, ColConfigurator> configurators = new HashMap<>();
        for (ColConfigurator c : colConfigurators) {
            // later configs override earlier configs at the same position
            configurators.put(c.srcPos(index), c);
        }

        int w = schema.getFields().size();
        Extractor<Object[], ?>[] extractors = new Extractor[w];
        for (int i = 0; i < w; i++) {
            ColConfigurator cc = configurators.computeIfAbsent(i, ii -> ColConfigurator.objectCol(ii, false));
            extractors[i] = cc.extractor(i, schema);
        }

        return extractors;
    }

}
