package org.dflib.agg;


public class PrimitiveSeriesMinMax {

    public static int minOfRange(int first) {
        return first;
    }

    public static int maxOfRange(int lastExclusive) {
        return lastExclusive - 1;
    }

    public static int minOfArray(int[] ints, int start, int len) {

        if (len == 0) {
            return 0; // is this reasonable?
        }

        int max = Integer.MAX_VALUE;

        for (int i = 0; i < len; i++) {

            int in = ints[start + i];
            if (in < max) {
                max = in;
            }
        }

        return max;
    }

    public static int maxOfArray(int[] ints, int start, int len) {

        if (len == 0) {
            return 0; // is this reasonable?
        }

        int max = Integer.MIN_VALUE;

        for (int i = 0; i < len; i++) {

            int in = ints[start + i];
            if (in > max) {
                max = in;
            }
        }

        return max;
    }

    public static long minOfArray(long[] longs, int start, int len) {

        if (len == 0) {
            return 0; // is this reasonable?
        }

        long max = Long.MAX_VALUE;

        for (int i = 0; i < len; i++) {

            long in = longs[start + i];
            if (in < max) {
                max = in;
            }
        }

        return max;
    }

    public static long maxOfArray(long[] longs, int start, int len) {

        if (len == 0) {
            return 0L; // is this reasonable?
        }

        long max = Long.MIN_VALUE;

        for (int i = 0; i < len; i++) {

            long in = longs[start + i];
            if (in > max) {
                max = in;
            }
        }

        return max;
    }

    /**
     * @since 1.1.0
     */
    public static float minOfArray(float[] vals, int start, int len) {

        if (len == 0) {
            return 0; // is this reasonable?
        }

        float min = Float.MAX_VALUE;

        for (int i = 0; i < len; i++) {

            float in = vals[start + i];
            if (in < min) {
                min = in;
            }
        }

        return min;
    }

    public static double minOfArray(double[] doubles, int start, int len) {

        if (len == 0) {
            return 0; // is this reasonable?
        }

        double max = Double.MAX_VALUE;

        for (int i = 0; i < len; i++) {

            double in = doubles[start + i];
            if (in < max) {
                max = in;
            }
        }

        return max;
    }

    /**
     * @since 1.1.0
     */
    public static float maxOfArray(float[] vals, int start, int len) {

        if (len == 0) {
            return 0L; // is this reasonable?
        }

        float max = Float.MIN_VALUE;

        for (int i = 0; i < len; i++) {

            float in = vals[start + i];
            if (in > max) {
                max = in;
            }
        }

        return max;
    }

    public static double maxOfArray(double[] doubles, int start, int len) {

        if (len == 0) {
            return 0L; // is this reasonable?
        }

        double max = Double.MIN_VALUE;

        for (int i = 0; i < len; i++) {

            double in = doubles[start + i];
            if (in > max) {
                max = in;
            }
        }

        return max;
    }
}
