package org.dflib;

import org.dflib.exp.bool.AndCondition;
import org.dflib.exp.bool.NotCondition;
import org.dflib.exp.bool.OrCondition;

/**
 * A {@link Exp} that evaluates to a BooleanSeries indicating whether the condition is true for any given
 * row of the source DataFrame.
 */
public interface Condition extends Exp<Boolean> {

    @Override
    BooleanSeries eval(DataFrame df);

    @Override
    BooleanSeries eval(Series<?> s);

    default int firstMatch(DataFrame df) {
        return eval(df).firstTrue();
    }

    default int firstMatch(Series<?> s) {
        return eval(s).firstTrue();
    }

    default Condition and(Condition c) {
        return new AndCondition(this, c);
    }

    default Condition or(Condition c) {
        return new OrCondition(this, c);
    }

    default Condition not() {
        return new NotCondition(this);
    }

    @Override
    default Class<Boolean> getType() {
        return Boolean.class;
    }

    @Override
    default Condition castAsBool() {
        return this;
    }
}
