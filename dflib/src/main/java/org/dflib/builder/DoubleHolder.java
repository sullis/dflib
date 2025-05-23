package org.dflib.builder;

public class DoubleHolder implements ValueHolder<Double> {

    private double v;

    @Override
    public Double get() {
        return v;
    }

    @Override
    public void push(Double v) {
        this.v = v != null ? v : 0.;
    }

    @Override
    public void pushDouble(double v) {
        this.v = v;
    }
}
