package com.nhl.dflib.exp.num;

import com.nhl.dflib.Exp;
import com.nhl.dflib.Series;
import com.nhl.dflib.exp.NumericExp;
import com.nhl.dflib.exp.UnaryExp;

import java.util.function.Function;

/**
 * @since 0.11
 */
public class IntUnaryExp<F> extends UnaryExp<F, Integer> implements NumericExp<Integer> {

    public IntUnaryExp(Exp<F> exp, Function<Series<F>, Series<Integer>> op) {
        super(exp, Integer.class, op);
    }
}