package org.dflib.parquet;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.dflib.DataFrame;
import org.dflib.Extractor;
import org.dflib.Index;
import org.dflib.builder.DataFrameAppender;
import org.dflib.parquet.read.DataframeParquetReaderBuilder;
import org.dflib.parquet.read.Row;
import org.dflib.parquet.read.RowExtractorFactory;

public class ParquetLoader {

    public DataFrame load(File file) {
        return load(file.toPath());
    }

    public DataFrame load(String filePath) {
        return load(new File(filePath));
    }

    public DataFrame load(Path filePath) {
        try {
            LocalInputFile localInputFile = new LocalInputFile(filePath);
            MessageType schema = null;
            try (ParquetFileReader parquetFile = ParquetFileReader.open(localInputFile)) {
                schema = parquetFile.getFileMetaData().getSchema();
            }
            DataFrameAppender<Row> appender = DataFrame.byRow(mapColumns(schema))
                    .columnIndex(createIndex(schema))
                    .appender();

            ParquetReader<Row> reader = new DataframeParquetReaderBuilder(localInputFile).build();

            Row row = null;
            while ((row = reader.read()) != null) {
                appender.append(row);
            }
            return appender.toDataFrame();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Index createIndex(GroupType schema) {
        String[] labels = schema.getFields().stream().map(Type::getName).toArray(String[]::new);
        return Index.of(labels);
    }

    private Extractor<Row, ?>[] mapColumns(GroupType schema) {
        List<Type> fields = schema.getFields();
        Extractor<Row, ?>[] extractors = new Extractor[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            extractors[i] = RowExtractorFactory.converterFor(fields.get(i), i);
        }
        return extractors;
    }

}
