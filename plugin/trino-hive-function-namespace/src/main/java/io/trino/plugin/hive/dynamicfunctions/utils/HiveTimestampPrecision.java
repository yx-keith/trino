package io.trino.plugin.hive.dynamicfunctions.utils;

public enum HiveTimestampPrecision
{
    MILLISECONDS(3), MICROSECONDS(6), NANOSECONDS(9);

    public static final HiveTimestampPrecision DEFAULT_PRECISION = MILLISECONDS;
    public static final HiveTimestampPrecision MAX = NANOSECONDS;

    private final int precision;

    HiveTimestampPrecision(int precision)
    {
        this.precision = precision;
    }

    public int getPrecision()
    {
        return precision;
    }
}
