package com.nhl.dflib.series;

import com.nhl.dflib.BooleanSeries;
import com.nhl.dflib.IntSeries;
import com.nhl.dflib.Series;
import com.nhl.dflib.collection.BooleanMutableList;
import com.nhl.dflib.collection.IntMutableList;
import com.nhl.dflib.collection.MutableList;
import com.nhl.dflib.concat.SeriesConcat;
import com.nhl.dflib.filter.ValuePredicate;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

public abstract class ObjectSeries<T> implements Series<T> {

    @Override
    public Class<?> getType() {
        return Object.class;
    }

    @Override
    public Series<T> rangeOpenClosed(int fromInclusive, int toExclusive) {

        if (fromInclusive == toExclusive) {
            return new EmptySeries<>();
        }

        return fromInclusive == 0 && toExclusive == size()
                ? this
                // RangeSeries does range checking
                : new RangeSeries<>(this, fromInclusive, toExclusive - fromInclusive);
    }

    @Override
    public Series<T> concat(Series<? extends T>... other) {
        if (other.length == 0) {
            return this;
        }

        Series<T>[] combined = new Series[other.length + 1];
        combined[0] = this;
        System.arraycopy(other, 0, combined, 1, other.length);

        return SeriesConcat.concat(combined);
    }

    @Override
    public Series<T> head(int len) {
        return len >= size() ? this : rangeOpenClosed(0, len);
    }

    @Override
    public Series<T> tail(int len) {
        int size = size();
        return len >= size ? this : rangeOpenClosed(size - len, size);
    }

    @Override
    public Series<T> select(IntSeries positions) {
        return new IndexedSeries<>(this, positions);
    }

    @Override
    public IntSeries index(ValuePredicate<T> predicate) {
        IntMutableList index = new IntMutableList();

        int len = size();

        for (int i = 0; i < len; i++) {
            if (predicate.test(get(i))) {
                index.add(i);
            }
        }

        return index.toIntSeries();
    }

    @Override
    public Series<T> replace(BooleanSeries condition, T with) {
        int s = size();
        int r = Math.min(s, condition.size());
        MutableList vals = new MutableList(s);

        for (int i = 0; i < r; i++) {
            vals.add(condition.getBoolean(i) ? with : get(i));
        }

        for (int i = r; i < s; i++) {
            vals.add(get(i));
        }

        return vals.toSeries();
    }

    @Override
    public Series<T> replaceNoMatch(BooleanSeries condition, T with) {

        int s = size();
        int r = Math.min(s, condition.size());
        MutableList vals = new MutableList(s);

        for (int i = 0; i < r; i++) {
            vals.add(condition.getBoolean(i) ? get(i) : with);
        }

        if (s > r) {
            vals.fill(r, s, with);
        }

        return vals.toSeries();
    }

    @Override
    public BooleanSeries eq(Series<T> another) {
        int s = size();
        int as = another.size();

        if (s != as) {
            throw new IllegalArgumentException("Another Series size " + as + " is not the same as this size " + s);
        }

        BooleanMutableList bools = new BooleanMutableList(s);
        for (int i = 0; i < s; i++) {
            bools.add(Objects.equals(get(i), another.get(i)));
        }

        return bools.toBooleanSeries();
    }

    @Override
    public BooleanSeries ne(Series<T> another) {
        int s = size();
        int as = another.size();

        if (s != as) {
            throw new IllegalArgumentException("Another Series size " + as + " is not the same as this size " + s);
        }

        BooleanMutableList bools = new BooleanMutableList(s);
        for (int i = 0; i < s; i++) {
            bools.add(!Objects.equals(get(i), another.get(i)));
        }

        return bools.toBooleanSeries();
    }

    @Override
    public Series<T> unique() {

        int size = size();
        if (size < 2) {
            return this;
        }

        Set<Object> unique = new LinkedHashSet<>();
        for (int i = 0; i < size; i++) {
            unique.add(get(i));
        }

        return unique.size() < size() ? new ArraySeries<>(unique.toArray((T[]) new Object[unique.size()])) : this;
    }

    @Override
    public String toString() {
        return ToString.toString(this);
    }
}
