package org.dflib;

import org.dflib.series.ObjectSeries;
import org.dflib.unit.DataFrameAsserts;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;


public class DataFrame_AddRowTest {

    @Test
    public void addRow() {
        DataFrame df = DataFrame.foldByRow("a", "b").of(
                1, "x",
                2, "y");

        DataFrame df1 = df.addRow(Map.of("a", 3, "b", "z"));
        new DataFrameAsserts(df1, "a", "b")
                .expectHeight(3)
                .expectRow(0, 1, "x")
                .expectRow(1, 2, "y")
                .expectRow(2, 3, "z");

        DataFrame df2 = df1.addRow(Map.of("a", 55, "b", "A"));
        new DataFrameAsserts(df2, "a", "b")
                .expectHeight(4)
                .expectRow(0, 1, "x")
                .expectRow(1, 2, "y")
                .expectRow(2, 3, "z")
                .expectRow(3, 55, "A");
    }

    @Test
    public void addRowMissingOrExtraValues() {
        DataFrame df = DataFrame.foldByRow("a", "b").of(
                1, "x",
                2, "y");

        DataFrame df1 = df.addRow(Map.of("c", 3, "b", "z"));
        new DataFrameAsserts(df1, "a", "b")
                .expectHeight(3)
                .expectRow(0, 1, "x")
                .expectRow(1, 2, "y")
                .expectRow(2, null, "z");
    }

    @Test
    public void addRow_PrimitiveColumns() {
        DataFrame df = DataFrame.byColumn("a", "b").of(
                Series.ofLong(5L, 6L),
                Series.ofInt(1, 2))
                .addRow(Map.of("a", 3L, "b", "str"));

        new DataFrameAsserts(df, "a", "b")
                .expectHeight(3)
                .expectRow(0, 5L, 1)
                .expectRow(1, 6L, 2)
                .expectRow(2, 3L, "str");

        assertInstanceOf(LongSeries.class, df.getColumn("a").unsafeCastAs(Long.class));
        assertInstanceOf(ObjectSeries.class, df.getColumn("b"));
    }
}
