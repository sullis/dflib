package com.nhl.dflib.builder;

import com.nhl.dflib.Extractor;
import com.nhl.dflib.Index;
import com.nhl.dflib.row.RowBuilder;
import com.nhl.dflib.row.RowProxy;

/**
 * A {@link RowProxy} with an internal buffer to store row values.
 *
 * @since 0.16
 */
public class ValueHolderProxy<S> implements RowProxy {

    private final Index index;
    private final ValueExtractor<S, ?>[] row;

    public ValueHolderProxy(Index index, Extractor<S, ?>[] extractors) {
        this.index = index;
        this.row = createRow(extractors);
    }

    private ValueExtractor<S, ?>[] createRow(Extractor<S, ?>[] extractors) {
        int w = extractors.length;
        ValueExtractor<S, ?>[] row = new ValueExtractor[w];
        for (int i = 0; i < w; i++) {
            row[i] = new ValueExtractor<>(extractors[i]);
        }

        return row;
    }

    public void reset(S source) {
        int w = row.length;
        for (int i = 0; i < w; i++) {
            row[i].reset(source);
        }
    }

    @Override
    public Index getIndex() {
        return index;
    }

    // TODO: ValueHolder and RowProxy should have primitive-aware getters (kinda like ValueStore primitive-aware "push)

    @Override
    public Object get(int columnPos) {
        return row[columnPos].holder.get();
    }

    @Override
    public Object get(String columnName) {
        return row[index.position(columnName)].holder.get();
    }

    @Override
    public int getInt(int columnPos) {
        // TODO: this code is unaware of primitive columns, and hence will do extra boxing/unboxing
        return (int) row[columnPos].holder.get();
    }

    @Override
    public int getInt(String columnName) {
        return getInt(index.position(columnName));
    }

    @Override
    public long getLong(int columnPos) {
        // TODO: this code is unaware of primitive columns, and hence will do extra boxing/unboxing
        return (long) row[columnPos].holder.get();
    }

    @Override
    public long getLong(String columnName) {
        return getLong(index.position(columnName));
    }

    @Override
    public double getDouble(int columnPos) {
        // TODO: this code is unaware of primitive columns, and hence will do extra boxing/unboxing
        return (double) row[columnPos].holder.get();
    }

    @Override
    public double getDouble(String columnName) {
        return getDouble(index.position(columnName));
    }

    @Override
    public boolean getBool(int columnPos) {
        // TODO: this code is unaware of primitive columns, and hence will do extra boxing/unboxing
        return (boolean) row[columnPos].holder.get();
    }

    @Override
    public boolean getBool(String columnName) {
        return getBool(index.position(columnName));
    }

    @Override
    public void copyRange(RowBuilder to, int fromOffset, int toOffset, int len) {
        // TODO: do we need this one for any reason?
        throw new UnsupportedOperationException("Not supported");
    }

    static class ValueExtractor<S, T> {

        private final Extractor<S, T> extractor;
        private final ValueHolder<T> holder;

        ValueExtractor(Extractor<S, T> extractor) {
            this.extractor = extractor;
            this.holder = extractor.createHolder();
        }

        void reset(S s) {
            extractor.extractAndStore(s, holder);
        }
    }
}
