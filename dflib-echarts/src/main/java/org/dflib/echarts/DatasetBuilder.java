package org.dflib.echarts;

import org.dflib.DataFrame;
import org.dflib.Series;
import org.dflib.echarts.render.ValueModels;
import org.dflib.echarts.render.option.dataset.DatasetModel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

class DatasetBuilder {

    private final DataFrame dataFrame;

    private final List<Series<?>> dataset;
    private final Map<String, Integer> datasetPosByDataColumn;
    private final Map<Integer, String> dataColumnByDatasetPos;

    DatasetBuilder(DataFrame dataFrame) {
        this.dataFrame = dataFrame;

        this.dataset = new ArrayList<>();
        this.datasetPosByDataColumn = new HashMap<>();
        this.dataColumnByDatasetPos = new HashMap<>();
    }

    // Append a row that is not present in the DataFrame.
    // Such rows may get duplicated, as there's no key that we can use to cache it
    int appendRow(Series<?> row) {
        int pos = dataset.size();
        dataset.add(Objects.requireNonNull(row));
        return pos;
    }

    int appendRow(String dataColumnName) {
        Objects.requireNonNull(dataColumnName);

        Integer existingPos = datasetPosByDataColumn.get(dataColumnName);
        if (existingPos != null) {
            return existingPos;
        }

        int pos = dataset.size();
        dataset.add(dataFrame.getColumn(dataColumnName));
        datasetPosByDataColumn.put(dataColumnName, pos);
        dataColumnByDatasetPos.put(pos, dataColumnName);
        return pos;
    }

    DatasetModel datasetModel() {

        if (dataset.isEmpty()) {
            return null;
        }

        // DF columns become rows and rows become columns in the EChart dataset
        int w = dataFrame.height();
        int h = dataset.size();

        Supplier<String> labelMaker = new Supplier<>() {

            String baseLabel = "L";
            int labelCounter = 0;
            Set<String> seen = new HashSet<>(datasetPosByDataColumn.keySet());

            @Override
            public String get() {
                String label;
                do {
                    label = baseLabel + labelCounter++;
                } while (!seen.add(label));

                return label;
            }
        };

        List<ValueModels<?>> rows = new ArrayList<>(h);
        for (int i = 0; i < h; i++) {

            List<Object> row = new ArrayList<>(w + 1);

            String dataColumn = dataColumnByDatasetPos.get(i);
            String rowLabel = dataColumn != null ? dataColumn : labelMaker.get();

            row.add(rowLabel);
            Series<?> rowData = dataset.get(i);

            for (int j = 0; j < w; j++) {
                row.add(rowData.get(j));
            }

            rows.add(ValueModels.of(row));
        }

        return new DatasetModel(ValueModels.of(rows));
    }
}
