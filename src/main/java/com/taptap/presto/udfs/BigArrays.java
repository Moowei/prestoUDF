package com.taptap.presto.udfs;

/**
 * @Description BigArrays
 * @Date 2019/12/31
 * @Author wangwei
 */
public final class BigArrays {
    private BigArrays() {}

    /**
     * Initial number of segments to support in array.
     */
    public static final int INITIAL_SEGMENTS = 1024;

    /**
     * The shift used to compute the segment associated with an index (equivalently, the logarithm of the segment size).
     */
    public static final int SEGMENT_SHIFT = 10;

    /**
     * Size of a single segment of a BigArray
     */
    public static final int SEGMENT_SIZE = 1 << SEGMENT_SHIFT;

    /**
     * The mask used to compute the offset associated to an index.
     */
    public static final int SEGMENT_MASK = SEGMENT_SIZE - 1;

    /**
     * Computes the segment associated with a given index.
     *
     * @param index an index into a big array.
     * @return the associated segment.
     */
    @SuppressWarnings("NumericCastThatLosesPrecision")
    public static int segment(long index)
    {
        return (int) (index >>> SEGMENT_SHIFT);
    }

    /**
     * Computes the offset associated with a given index.
     *
     * @param index an index into a big array.
     * @return the associated offset (in the associated {@linkplain #segment(long) segment}).
     */
    @SuppressWarnings("NumericCastThatLosesPrecision")
    public static int offset(long index)
    {
        return (int) (index & SEGMENT_MASK);
    }
}
