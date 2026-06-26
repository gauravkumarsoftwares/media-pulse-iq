package com.java.query.dto;

/**
 * A single bucketed data point in a campaign metric time series.
 *
 * @param timestamp ISO-8601 bucket start time
 * @param value     aggregated count for this bucket
 */
public record TimeSeriesPoint(String timestamp, long value) {}

