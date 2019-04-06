package com.nhl.dflib.map;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@FunctionalInterface
public interface ValueMapper<V, VR> {

    static ValueMapper<String, Integer> stringToInt() {
        return s -> s != null ? Integer.valueOf(s) : null;
    }

    static ValueMapper<String, String> stringToString() {
        return s -> s;
    }

    static ValueMapper<String, Long> stringToLong() {
        return s -> s != null ? Long.valueOf(s) : null;
    }

    static ValueMapper<String, Double> stringToDouble() {
        return s -> s != null ? Double.valueOf(s) : null;
    }

    static ValueMapper<String, LocalDate> stringToDate() {
        return s -> s != null ? LocalDate.parse(s) : null;
    }

    static ValueMapper<String, LocalDate> stringToDate(DateTimeFormatter formatter) {
        return s -> s != null ? LocalDate.parse(s, formatter) : null;
    }

    static ValueMapper<String, LocalDateTime> stringToDateTime() {
        return s -> s != null ? LocalDateTime.parse(s) : null;
    }

    static ValueMapper<String, LocalDateTime> stringToDateTime(DateTimeFormatter formatter) {
        return s -> s != null ? LocalDateTime.parse(s, formatter) : null;
    }

    VR map(V v);
}
