package org.dflib;

import org.dflib.unit.DataFrameAsserts;
import org.junit.jupiter.api.Test;

import static org.dflib.Exp.*;

public class ColumnSet_AggVConcat {

    @Test
    public void vConcat() {
        DataFrame df = DataFrame.foldByRow("a", "b").of(
                1, "x",
                0, "a");

        DataFrame agg = df.cols().agg(
                $col("a").vConcat("_"),
                $col(1).vConcat(" ", "[", "]"));

        new DataFrameAsserts(agg, "a", "b")
                .expectHeight(1)
                .expectRow(0, "1_0", "[x a]");
    }

    @Test
    public void filtered() {
        DataFrame df = DataFrame.foldByRow("a", "b").of(
                7, 1,
                -1, 5,
                -4, 5,
                8, 8);

        DataFrame agg = df.cols().agg(
                $col(1).vConcat($int(0).mod(2).eq(0), "_"),
                $col("a").vConcat($int("b").mod(2).eq(1), ", ", "[", "]"));

        new DataFrameAsserts(agg, "b", "a")
                .expectHeight(1)
                .expectRow(0, "5_8", "[7, -1, -4]");
    }
}
