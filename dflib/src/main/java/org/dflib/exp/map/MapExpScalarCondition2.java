package org.dflib.exp.map;

import org.dflib.BooleanSeries;
import org.dflib.Exp;
import org.dflib.Series;
import org.dflib.builder.BoolAccum;
import org.dflib.exp.ExpScalarCondition2;
import org.dflib.series.FalseSeries;

import java.util.function.BiFunction;
import java.util.function.BiPredicate;


public class MapExpScalarCondition2<L, R> extends ExpScalarCondition2<L, R> {

    private final BiFunction<Series<L>, R, BooleanSeries> op;

    public static <L, R> MapExpScalarCondition2<L, R> map(
            String opName, Exp<L> left, R right, BiFunction<Series<L>, R, BooleanSeries> op) {
        return new MapExpScalarCondition2<>(opName, left, right, op);
    }

    public static <L, R> MapExpScalarCondition2<L, R> mapVal(String opName, Exp<L> left, R right, BiPredicate<L, R> predicate) {
        return new MapExpScalarCondition2<>(opName, left, right, valToSeries(predicate));
    }

    protected static <L, R> BiFunction<Series<L>, R, BooleanSeries> valToSeries(BiPredicate<L, R> predicate) {
        return (ls, r) -> {

            if (r == null) {
                return new FalseSeries(ls.size());
            }

            int len = ls.size();
            BoolAccum accum = new BoolAccum(len);
            for (int i = 0; i < len; i++) {
                L l = ls.get(i);
                accum.pushBool(l != null ? predicate.test(l, r) : false);
            }

            return accum.toSeries();
        };
    }

    protected MapExpScalarCondition2(String opName, Exp<L> left, R right, BiFunction<Series<L>, R, BooleanSeries> op) {
        super(opName, left, right);
        this.op = op;
    }

    @Override
    protected BooleanSeries doEval(Series<L> left, R right) {
        return op.apply(left, right);
    }
}
