package org.dflib.exp.num;

import org.dflib.BooleanSeries;
import org.dflib.Condition;
import org.dflib.DataFrame;
import org.dflib.DecimalExp;
import org.dflib.NumExp;
import org.dflib.Series;
import org.dflib.StrExp;
import org.dflib.unit.BoolSeriesAsserts;
import org.dflib.unit.SeriesAsserts;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.dflib.Exp.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

public class DecimalColumnTest {

    @Test
    public void getColumnName() {
        assertEquals("a", $decimal("a").getColumnName());
        assertEquals("$decimal(0)", $decimal(0).getColumnName());
    }

    @Test
    public void getColumnName_DataFrame() {
        DataFrame df = DataFrame.foldByRow("a", "b").of();
        assertEquals("b", $decimal("b").getColumnName(df));
        assertEquals("a", $decimal(0).getColumnName(df));
    }

    @Test
    public void eval() {
        DecimalExp exp = $decimal("b");

        DataFrame df = DataFrame.foldByRow("a", "b", "c").of(
                "1", new BigDecimal("2.0100287"), new BigDecimal("2.010029"),
                "4", new BigDecimal("2.01001"), new BigDecimal("2.01003"));

        new SeriesAsserts(exp.eval(df)).expectData(new BigDecimal("2.0100287"), new BigDecimal("2.01001"));
    }

    @Test
    public void chainStaysDecimal() {
        DecimalExp exp = $decimal("b").as("x").as("y").sum().as("SUM(x)");
        assertEquals("SUM(x)", exp.getColumnName(mock(DataFrame.class)));
    }

    @Test
    public void as() {
        DecimalExp exp = $decimal("b");
        assertEquals("b", exp.getColumnName(mock(DataFrame.class)));
        assertEquals("c", exp.as("c").getColumnName(mock(DataFrame.class)));
    }

    @Test
    public void round() {
        DataFrame df = DataFrame.foldByRow("a").of(
                new BigDecimal("2.0100287"),
                new BigDecimal("4.5"),
                new BigDecimal("0.00005"),
                new BigDecimal("1.4999999"),
                new BigDecimal("-0.00005"),
                new BigDecimal("-2.0100287"));

        Series<BigDecimal> s = $decimal("a").round().eval(df);
        new SeriesAsserts(s).expectData(
                new BigDecimal("2"),
                new BigDecimal("5"),
                new BigDecimal("0"),
                new BigDecimal("1"),
                new BigDecimal("0"),
                new BigDecimal("-2"));
    }

    @Test
    public void scale() {
        DataFrame df = DataFrame.foldByRow("a").of(
                new BigDecimal("2.0100287"),
                new BigDecimal("4.5"));

        Series<BigDecimal> s = $decimal("a").scale(2).eval(df);
        new SeriesAsserts(s).expectData(new BigDecimal("2.01"), new BigDecimal("4.50"));
    }

    @Test
    public void castAsDecimal() {
        DecimalExp e = $decimal("a");
        assertSame(e, e.castAsDecimal());
    }

    @Test
    public void castAsDecimal_DataFrame() {
        DataFrame df = DataFrame.foldByRow("a").of(
                new BigDecimal("2.0100287"),
                new BigDecimal("4.5"));

        Series<BigDecimal> s = $decimal("a").castAsDecimal().eval(df);
        new SeriesAsserts(s).expectData(new BigDecimal("2.0100287"), new BigDecimal("4.5"));
    }

    @Test
    public void castAsStr() {
        StrExp str = $decimal(0).castAsStr();
        Series<BigDecimal> s = Series.of(new BigDecimal("5.01"), null);
        new SeriesAsserts(str.eval(s)).expectData("5.01", null);
    }

    @Test
    public void add_IntPrimitive() {

        DecimalExp exp = $decimal("a").add(1).scale(2);

        DataFrame df = DataFrame.foldByRow("a").of(
                new BigDecimal("2.0100287"),
                new BigDecimal("4.5"));

        new SeriesAsserts(exp.eval(df)).expectData(new BigDecimal("3.01"), new BigDecimal("5.50"));
    }

    @Test
    public void cumSum() {
        DecimalExp exp = $decimal("a").cumSum();

        DataFrame df = DataFrame.foldByRow("a").of(
                null,
                new BigDecimal("2.01"),
                new BigDecimal("4.59"),
                null,
                new BigDecimal("11.21"),
                new BigDecimal("-12.16"));

        new SeriesAsserts(exp.eval(df)).expectData(
                null,
                new BigDecimal("2.01"),
                new BigDecimal("6.60"),
                null,
                new BigDecimal("17.81"),
                new BigDecimal("5.65")
        );
    }

    @Test
    public void sum() {
        DecimalExp exp = $decimal("a").sum().scale(2);

        DataFrame df = DataFrame.foldByRow("a").of(
                new BigDecimal("2.0100287"),
                new BigDecimal("4.5"));

        assertEquals(new BigDecimal("6.51"), exp.reduce(df));
    }

    @Test
    public void sum_Nulls() {
        DecimalExp exp = $decimal("a").sum().scale(2);

        DataFrame df = DataFrame.foldByRow("a").of(
                new BigDecimal("2.0100287"),
                null,
                new BigDecimal("4.5"));

        assertEquals(new BigDecimal("6.51"), exp.reduce(df));
    }

    @Test
    public void median_Zero() {
        DecimalExp exp = $decimal(0).median().scale(3);
        Series<BigDecimal> s = Series.of();
        assertNull(exp.reduce(s));
    }

    @Test
    public void median_One() {
        DecimalExp exp = $decimal(0).median().scale(3);
        Series<BigDecimal> s = Series.of(new BigDecimal("100.01"));
        assertEquals(new BigDecimal("100.010"), exp.reduce(s));
    }

    @Test
    public void median_Odd() {
        DecimalExp exp = $decimal(0).median().scale(3);

        Series<BigDecimal> s = Series.of(new BigDecimal("100.01"), new BigDecimal("55.5"), new BigDecimal("0."));

        assertEquals(new BigDecimal("55.500"), exp.reduce(s));
    }

    @Test
    public void median_Even() {
        DecimalExp exp = $decimal(0).median().scale(1);

        Series<BigDecimal> s = Series.of(
                new BigDecimal("100.01"), new BigDecimal("55.5"), new BigDecimal("0."), new BigDecimal("5."));

        assertEquals(new BigDecimal("30.3"), exp.reduce(s));
    }

    @Test
    public void median_Nulls() {
        DecimalExp exp = $decimal(0).median().scale(3);

        Series<BigDecimal> s = Series.of(
                new BigDecimal("100.01"), null, new BigDecimal("55.5"), new BigDecimal("0."));

        assertEquals(new BigDecimal("55.500"), exp.reduce(s));
    }

    @Test
    public void quantile_Nulls() {
        DecimalExp exp = $decimal(0).quantile(0.75).scale(3);

        Series<BigDecimal> s = Series.of(
                new BigDecimal("100.01"), null, new BigDecimal("55.5"), new BigDecimal("0."), new BigDecimal("-220.2"), new BigDecimal("34.8"), new BigDecimal("8.1"));

        assertEquals(new BigDecimal("50.325"), exp.reduce(s));
    }

    @Test
    public void add_Decimal() {
        DataFrame df = DataFrame.foldByRow("a", "b").of(
                new BigDecimal("1.01"), new BigDecimal("2."),
                new BigDecimal("3."), new BigDecimal("4.5"));

        Series<? extends Number> s = $decimal("b").add($decimal("a")).eval(df);
        new SeriesAsserts(s).expectData(new BigDecimal("3.01"), new BigDecimal("7.5"));
    }

    @Test
    public void divide_Int() {
        DataFrame df = DataFrame.foldByRow("a", "b").of(
                new BigDecimal("35"), 2,
                new BigDecimal("3.3"), 3);

        Series<? extends Number> s = $decimal("a").div($int("b")).eval(df);
        new SeriesAsserts(s).expectData(new BigDecimal("17.5"), new BigDecimal("1.1"));
    }

    @Test
    public void divide_Double() {
        DataFrame df = DataFrame.foldByRow("a", "b").of(
                new BigDecimal("5.0"), 2.5,
                new BigDecimal("3.3"), 3.33);

        Series<? extends Number> s = $decimal("a").div($double("b")).eval(df);
        new SeriesAsserts(s).expectData(new BigDecimal("2"), new BigDecimal("0.99099099099099096984560210653599037467692134485397"));
    }


    @Test
    public void gT_Decimal() {
        Condition c = $decimal("a").gt($decimal("b"));

        DataFrame df = DataFrame.foldByRow("a", "b").of(
                new BigDecimal("1.1"), new BigDecimal("1.0001"),
                new BigDecimal("3"), new BigDecimal("3"),
                new BigDecimal("1.1"), new BigDecimal("1.2"));

        new BoolSeriesAsserts(c.eval(df)).expectData(true, false, false);
    }

    @Test
    public void abs() {
        DataFrame df = DataFrame.foldByRow("a").of(
                new BigDecimal("-5.1"),
                BigDecimal.ZERO,
                new BigDecimal("11.5"));

        Series<? extends Number> s = $decimal("a").abs().eval(df);
        new SeriesAsserts(s).expectData(new BigDecimal("5.1"), BigDecimal.ZERO, new BigDecimal("11.5"));
    }

    @Test
    public void ne() {

        DataFrame df = DataFrame.foldByRow("a").of(
                new BigDecimal("-5.1"),
                BigDecimal.ZERO,
                new BigDecimal("11.5"));

        BooleanSeries s = $decimal("a").ne(new BigDecimal("11.5")).eval(df);
        new BoolSeriesAsserts(s).expectData(true, true, false);
    }

    @Test
    public void eq() {

        DataFrame df = DataFrame.foldByRow("a").of(
                new BigDecimal("-5.1"),
                BigDecimal.ZERO,
                new BigDecimal("11.5"));

        BooleanSeries s = $decimal("a").eq(new BigDecimal("11.5")).eval(df);
        new BoolSeriesAsserts(s).expectData(false, false, true);
    }

    @Test
    public void eq_Zero() {

        DataFrame df = DataFrame.foldByRow("a").of(
                new BigDecimal("-5.1"),
                BigDecimal.ZERO,
                new BigDecimal("0"),
                new BigDecimal("11.5"));

        BooleanSeries s = $decimal("a").eq(BigDecimal.ZERO).eval(df);
        new BoolSeriesAsserts(s).expectData(false, true, true, false);
    }

    @Test
    public void ne_NonNumber() {

        DataFrame df = DataFrame.foldByRow("a").of(
                new BigDecimal("-5.1"),
                BigDecimal.ZERO,
                new BigDecimal("11.5"));

        BooleanSeries s = $decimal("a").ne("11.5").eval(df);
        new BoolSeriesAsserts(s).expectData(true, true, true);
    }

    @Test
    public void cumSum_getColumnName() {
        NumExp<?> exp = $decimal("a").cumSum();
        assertEquals("cumSum(a)", exp.getColumnName());
    }

    @Test
    public void sum_getColumnName() {
        NumExp<?> exp = $decimal("a").sum();
        assertEquals("sum(a)", exp.getColumnName());
    }

    @Test
    public void castAsBool() {
        Condition c = $decimal(0).castAsBool();

        Series<BigDecimal> s = Series.of(
                new BigDecimal("-5.1"),
                BigDecimal.ZERO,
                null,
                new BigDecimal("11.5"));
        new BoolSeriesAsserts(c.eval(s)).expectData(true, false, false, true);
    }

    @Test
    public void mapBoolVal() {
        Condition c = $decimal(0).mapBoolVal(d -> d.doubleValue() > 0);

        Series<BigDecimal> s = Series.of(
                new BigDecimal("-5.1"),
                BigDecimal.ZERO,
                null,
                new BigDecimal("11.5"));
        new BoolSeriesAsserts(c.eval(s)).expectData(false, false, false, true);
    }

    @Test
    public void mapBool() {
        Condition c = $decimal(0).mapBool(Series::isNotNull);

        Series<BigDecimal> s = Series.of(
                new BigDecimal("-5.1"),
                BigDecimal.ZERO,
                null,
                new BigDecimal("11.5"));
        new BoolSeriesAsserts(c.eval(s)).expectData(true, true, false, true);
    }

    @Test
    public void between() {
        Condition c = $decimal("a").between(new BigDecimal("1.0"), new BigDecimal("2.01"));

        Series<BigDecimal> s = Series.of(
                new BigDecimal("0.99"),
                new BigDecimal("1.0"),
                new BigDecimal("1.5"),
                new BigDecimal("2.01"),
                new BigDecimal("2.02"));

        // run and verify the calculation
        new BoolSeriesAsserts(c.eval(s)).expectData(false, true, true, true, false);
    }
}
