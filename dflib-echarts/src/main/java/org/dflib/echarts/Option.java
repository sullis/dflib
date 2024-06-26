package org.dflib.echarts;

import org.dflib.DataFrame;
import org.dflib.Series;
import org.dflib.echarts.render.OptionModel;
import org.dflib.echarts.render.ValueModel;
import org.dflib.echarts.render.option.DataSetModel;
import org.dflib.echarts.render.option.GridModel;
import org.dflib.echarts.render.option.RowModel;
import org.dflib.echarts.render.option.SeriesModel;
import org.dflib.series.IntSequenceSeries;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A builder of the EChart "option" object - the main chart configuration.
 *
 * @since 1.0.0-M21
 */
public class Option {

    private static final String DEFAULT_LABELS_LABEL = "labels";

    private String title;
    private Boolean legend;
    private Toolbox toolbox;
    private Tooltip tooltip;
    private List<Grid> grids;
    private List<BoundXAxis> xAxes;
    private List<YAxis> yAxes;

    private final Map<String, BoundSeries> series;
    private SeriesOpts defaultSeriesOpts;

    /**
     * @since 1.0.0-M22
     */
    public static Option of() {
        return new Option();
    }

    protected Option() {
        // keeping the "series" order predictable
        this.series = new LinkedHashMap<>();
    }

    public Option toolbox(Toolbox toolbox) {
        this.toolbox = toolbox;
        return this;
    }

    /**
     * @since 1.0.0-M22
     */
    public Option tooltip(Tooltip tooltip) {
        this.tooltip = tooltip;
        return this;
    }

    /**
     * Adds a Grid to the chart. Grids are used to plot multiple charts in cartesian coordinates. Axis objects can
     * optionally have grid references.
     *
     * @since 1.0.0-M22
     */
    public Option grid(Grid grid) {
        Objects.requireNonNull(grid);

        if (this.grids == null) {
            this.grids = new ArrayList<>();
        }

        this.grids.add(grid);
        return this;
    }

    /**
     * Adds an X axis to the chart, that will use the specified DataFrame column as axis labels. If no X axis is set,
     * series element indices will be used for X.
     */
    public Option xAxis(String dataColumn) {
        return xAxis(dataColumn, XAxis.ofDefault());
    }

    public Option xAxis(XAxis axis) {
        return xAxis(null, axis);
    }

    public Option xAxis(String dataColumn, XAxis axis) {
        Objects.requireNonNull(dataColumn);

        if (xAxes == null) {
            xAxes = new ArrayList<>(3);
        }

        xAxes.add(new BoundXAxis(dataColumn, axis));
        return this;
    }


    /**
     * Adds one or more Y axes to the chart.
     *
     * @since 1.0.0-M22
     */
    public Option yAxes(YAxis... axes) {
        for (YAxis a : axes) {
            yAxis(a);
        }

        return this;
    }

    /**
     * Adds a Y axis to the chart.
     */
    public Option yAxis(YAxis axis) {
        Objects.requireNonNull(axis);

        if (this.yAxes == null) {
            this.yAxes = new ArrayList<>(3);
        }

        this.yAxes.add(axis);
        return this;
    }

    /**
     * Sets a template to be used with all data series that don't have their own explicit options.
     */
    public Option defaultSeriesOpts(SeriesOpts opts) {
        this.defaultSeriesOpts = opts;
        return this;
    }

    /**
     * Specifies one or more DataFrame columns to be plotted as individual series. Sets series options.
     */
    public Option series(SeriesOpts opts, String... dataColumns) {
        for (String c : dataColumns) {
            series.put(c, new BoundSeries(c, opts));
        }

        return this;
    }

    /**
     * Specifies one or more DataFrame columns to be plotted as individual series. Series will be rendered with default
     * options.
     */
    public Option series(String... dataColumns) {
        for (String c : dataColumns) {
            series.put(c, new BoundSeries(c, null));
        }

        return this;
    }

    public Option title(String title) {
        this.title = title;
        return this;
    }

    public Option legend() {
        this.legend = Boolean.TRUE;
        return this;
    }

    protected OptionModel resolve(DataFrame df) {

        boolean cartesianDefaults = useCartesianDefaults();
        List<XAxis> xs = xAxes != null
                ? xAxes.stream().map(BoundXAxis::getAxis).collect(Collectors.toList())
                : (cartesianDefaults ? List.of(XAxis.ofDefault()) : null);
        List<YAxis> ys = yAxes != null ? yAxes : (cartesianDefaults ? List.of(YAxis.ofDefault()) : null);
        DataSetLabels labels = datasetLabels(df, cartesianDefaults);
        DataSetModel dataset = dataset(df, labels.rows);

        return new OptionModel(
                dataset,
                this.legend != null ? this.legend : false,
                grids(),
                datasetSeries(labels),
                this.title,
                this.toolbox != null ? this.toolbox.resolve() : null,
                this.tooltip != null ? this.tooltip.resolve() : null,
                xs != null ? xs.stream().map(XAxis::resolve).collect(Collectors.toList()) : null,
                ys != null ? ys.stream().map(YAxis::resolve).collect(Collectors.toList()) : null
        );
    }

    protected boolean useCartesianDefaults() {
        return series.isEmpty()
                || series.values().stream().filter(BoundSeries::isCartesianOrNull).findFirst().isPresent();
    }

    protected List<GridModel> grids() {
        return grids != null
                ? grids.stream().map(Grid::resolve).collect(Collectors.toList())
                : null;
    }

    protected DataSetModel dataset(DataFrame df, List<RowModel> labels) {

        // DF columns become rows and rows become columns in the EChart dataset
        int w = df.height();
        int h = series.size();

        List<RowModel> rows = new ArrayList<>(h + labels.size());
        rows.addAll(labels);

        String[] rowLabels = series.keySet().toArray(new String[0]);
        for (int i = 0; i < h; i++) {
            List<ValueModel> row = new ArrayList<>(w + 1);
            row.add(new ValueModel(rowLabels[i], w == 0));
            Series<?> data = df.getColumn(rowLabels[i]);

            for (int j = 0; j < w; j++) {
                row.add(new ValueModel(data.get(j), j + 1 == w));
            }

            rows.add(new RowModel(row, i + 1 == h));
        }

        return new DataSetModel(rows);
    }

    protected DataSetLabels datasetLabels(DataFrame df, boolean cartesianDefaults) {

        Map<String, List<ValueModel>> rowMap = new LinkedHashMap<>();

        Map<Integer, Integer> rowPosByXAxisIndex = new HashMap<>();
        Map<String, Integer> rowPosByRowLabel = new HashMap<>();

        // The first source of labels - a column associated with XAxis.
        if (xAxes != null) {

            // If multiple Axis point to the same data column, we'll generate only one dataset row for it
            int xlen = xAxes.size();
            for (int i = 0; i < xlen; i++) {
                String columnName = xAxes.get(i).columnName;
                String labelsName = labelsName(columnName);

                Integer existingPos = rowPosByRowLabel.get(labelsName);
                if (existingPos != null) {
                    rowPosByXAxisIndex.put(i, existingPos);
                } else {
                    rowPosByRowLabel.put(labelsName, i);
                    rowPosByXAxisIndex.put(i, i);
                    rowMap.put(labelsName, datasetLabelRow(df, columnName, labelsName));
                }
            }

        } else if (cartesianDefaults) {
            String labelsName = labelsName(null);
            List<ValueModel> xAxisLabels = datasetLabelRow(df, null, labelsName);
            rowMap.put((String) xAxisLabels.get(0).getValue(), xAxisLabels);
        }

        // the next source of labels - columns associated with pie charts
        for (BoundSeries s : series.values()) {

            if (s.opts instanceof PieSeriesOpts) {
                PieSeriesOpts pco = (PieSeriesOpts) s.opts;
                String labelsName = labelsName(pco.getLabelColumn());
                List<ValueModel> pieLabels = datasetLabelRow(df, pco.getLabelColumn(), labelsName);
                String key = (String) pieLabels.get(0).getValue();

                rowMap.putIfAbsent(key, pieLabels);
            }
        }

        int len = rowMap.size();
        List<RowModel> rows = new ArrayList<>(len);

        int[] i = new int[1];
        rowMap.forEach((k, v) -> rows.add(new RowModel(v, i[0]++ == len)));

        return new DataSetLabels(rowPosByXAxisIndex, rows);
    }

    protected String labelsName(String columnName) {
        return columnName != null ? columnName : DEFAULT_LABELS_LABEL;
    }

    protected List<ValueModel> datasetLabelRow(DataFrame df, String columnName, String labelsName) {

        // DF columns become rows and rows become columns in the EChart dataset
        int w = df.height();

        List<ValueModel> row = new ArrayList<>(w + 1);

        Series<?> columnLabels = columnName != null
                ? df.getColumn(columnName)
                : new IntSequenceSeries(1, df.height() + 1);

        row.add(new ValueModel(labelsName, w == 0));
        for (int i = 0; i < w; i++) {
            row.add(new ValueModel(columnLabels.get(i), i + 1 == w));
        }

        return row;
    }

    protected List<SeriesModel> datasetSeries(DataSetLabels labels) {
        SeriesOpts baseOpts = baseSeriesOptsTemplate();
        int len = series.size();

        // Series data rows follow label rows. So apply the offset to the "y" of the encoder
        int y = labels.size();

        List<SeriesModel> models = new ArrayList<>(len);
        for (BoundSeries s : series.values()) {

            int labelsPos;

            if (s.opts instanceof CartesianSeriesOpts) {
                labelsPos = labels.xForXAxis(((CartesianSeriesOpts) s.opts).xAxisIndex);
            } else if (s.opts instanceof PieSeriesOpts) {
                // TODO: PieChart label to column resolution (other than 0 would not work)
                labelsPos = 0;
            } else {
                labelsPos = 0;
            }

            SeriesModel m = s.fillOpts(baseOpts).resolve(labelsPos, y++);
            models.add(m);
        }

        return models;
    }

    protected SeriesOpts baseSeriesOptsTemplate() {
        return this.defaultSeriesOpts != null ? defaultSeriesOpts : SeriesOpts.ofLine();
    }

    static class DataSetLabels {
        final List<RowModel> rows;
        final Map<Integer, Integer> xAxisIndices;

        DataSetLabels(Map<Integer, Integer> xAxisIndices, List<RowModel> rows) {
            this.xAxisIndices = xAxisIndices;
            this.rows = rows;
        }

        int size() {
            return rows.size();
        }

        int xForXAxis(Integer xAxisIndex) {
            return xAxisIndex != null ? xAxisIndices.get(xAxisIndex) : 0;
        }
    }

    static class BoundXAxis {
        final String columnName;
        final XAxis axis;

        BoundXAxis(String columnName, XAxis axis) {
            this.columnName = columnName;
            this.axis = axis;
        }

        XAxis getAxis() {
            return axis;
        }
    }

    static class BoundSeries {
        final String columnName;
        final SeriesOpts opts;

        BoundSeries(String columnName, SeriesOpts opts) {
            this.columnName = Objects.requireNonNull(columnName);
            this.opts = opts;
        }

        BoundSeries fillOpts(SeriesOpts defaultOpts) {
            return opts != null ? this : new BoundSeries(columnName, defaultOpts);
        }

        boolean isCartesianOrNull() {
            return opts == null || opts instanceof CartesianSeriesOpts;
        }

        SeriesModel resolve(int labelsPos, int seriesPos) {
            return opts.resolve(
                    columnName,
                    labelsPos,
                    seriesPos,
                    // hardcoding "row" series layout. It corresponds to the dataset layout
                    "row"
            );
        }
    }
}
