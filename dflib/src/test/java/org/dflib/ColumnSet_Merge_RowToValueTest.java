package org.dflib;

import org.dflib.unit.DataFrameAsserts;
import org.junit.jupiter.api.Test;

public class ColumnSet_Merge_RowToValueTest {

    @Test
    public void colsAppend() {
        DataFrame df = DataFrame.foldByRow("a", "b")
                .of(1, "x", 2, "y")
                .colsAppend("b", "c")
                .merge(r -> r.get(0, Integer.class) * 100, r -> r.get(0, Integer.class) * 10);

        new DataFrameAsserts(df, "a", "b", "b_", "c")
                .expectHeight(2)
                .expectRow(0, 1, "x", 100, 10)
                .expectRow(1, 2, "y", 200, 20);
    }

    @Test
    public void cols_ByName() {
        DataFrame df = DataFrame.foldByRow("a", "b")
                .of(1, "x", 2, "y")
                .cols("b", "c").merge(r -> r.get(0, Integer.class) * 100, r -> r.get(0, Integer.class) * 10);

        new DataFrameAsserts(df, "a", "b", "c")
                .expectHeight(2)
                .expectRow(0, 1, 100, 10)
                .expectRow(1, 2, 200, 20);
    }

    @Test
    public void cols_ByPos() {
        DataFrame df = DataFrame.foldByRow("a", "b")
                .of(1, "x", 2, "y")
                .cols(1, 2).merge(r -> r.get(0, Integer.class) * 100, r -> r.get(0, Integer.class) * 10);

        new DataFrameAsserts(df, "a", "b", "2")
                .expectHeight(2)
                .expectRow(0, 1, 100, 10)
                .expectRow(1, 2, 200, 20);
    }

    @Test
    public void cols() {
        DataFrame df = DataFrame.foldByRow("a", "b")
                .of(1, "x", 2, "y")
                .cols().merge(r -> r.get(0, Integer.class) * 100, r -> r.get(0, Integer.class) * 10);

        new DataFrameAsserts(df, "a", "b", "2", "3")
                .expectHeight(2)
                .expectRow(0, 1, "x", 100, 10)
                .expectRow(1, 2, "y", 200, 20);
    }

    @Test
    public void colsExcept_ByName() {
        DataFrame df = DataFrame.foldByRow("a", "b", "c")
                .of(1, "x", "z", 2, "y", "a")
                .colsExcept("a").merge(r -> r.get(0, Integer.class) * 100, r -> r.get(0, Integer.class) * 10);

        new DataFrameAsserts(df, "a", "b", "c")
                .expectHeight(2)
                .expectRow(0, 1, 100, 10)
                .expectRow(1, 2, 200, 20);
    }

    @Test
    public void colsExcept_ByPos() {
        DataFrame df = DataFrame.foldByRow("a", "b", "c")
                .of(1, "x", "z", 2, "y", "a")
                .colsExcept(1).merge(r -> r.get(0, Integer.class) * 100, r -> r.get(0, Integer.class) * 10);

        new DataFrameAsserts(df, "a", "b", "c")
                .expectHeight(2)
                .expectRow(0, 100, "x", 10)
                .expectRow(1, 200, "y", 20);
    }
}
