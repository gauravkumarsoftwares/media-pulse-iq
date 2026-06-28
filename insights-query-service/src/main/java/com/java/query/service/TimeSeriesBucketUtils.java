package com.java.query.service;

import com.java.query.config.QueryProperties;
import com.java.query.dto.TimeSeriesPoint;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * Time-bucketing utilities; stateless aside from the configurable bucket cap.
 *
 * <p>Converted from a static utility to a Spring {@code @Component} so that
 * {@code platform.query.max-buckets} (default 10 000) is injected via
 * {@link QueryProperties} instead of being hard-coded (BUCKET-CAP fix).
 *
 * <p>Handlers that use the even-distribution approximation inject this bean;
 * the cap is therefore tunable per environment without recompiling.
 */
@Component
@RequiredArgsConstructor
public final class TimeSeriesBucketUtils {

    private final QueryProperties queryProperties;

    /**
     * Distribute {@code total} evenly across grain-aligned buckets between
     * {@code from} and {@code to}. The <strong>last bucket absorbs any remainder</strong>.
     *
     * @param from  window start (inclusive)
     * @param to    window end   (inclusive)
     * @param grain {@code minute}, {@code hour} (default), or {@code day}
     * @param total scalar aggregate to distribute
     * @return ordered list of {@link TimeSeriesPoint}; single fallback point if no buckets
     */
    public List<TimeSeriesPoint> distributeEvenly(Instant from, Instant to,
                                                   String grain, long total) {
        List<Instant> buckets = computeBuckets(from, to, grain);
        if (buckets.isEmpty()) {
            return List.of(new TimeSeriesPoint(to.toString(), total));
        }

        long perBucket = total / buckets.size();
        long remainder = total % buckets.size();
        int  last      = buckets.size() - 1;

        List<TimeSeriesPoint> series = new ArrayList<>(buckets.size());
        for (int i = 0; i < buckets.size(); i++) {
            long value = perBucket + (i == last ? remainder : 0);
            series.add(new TimeSeriesPoint(buckets.get(i).toString(), value));
        }
        return series;
    }

    /**
     * Compute grain-aligned bucket start instants between {@code from} and {@code to}.
     * Capped at {@code platform.query.max-buckets} (default 10 000).
     */
    List<Instant> computeBuckets(Instant from, Instant to, String grain) {
        ChronoUnit unit = switch (grain.toLowerCase()) {
            case "minute" -> ChronoUnit.MINUTES;
            case "day"    -> ChronoUnit.DAYS;
            default       -> ChronoUnit.HOURS;
        };

        int           maxBuckets = queryProperties.getMaxBuckets();
        List<Instant> buckets    = new ArrayList<>();
        ZonedDateTime cursor     = from.atZone(ZoneOffset.UTC).truncatedTo(unit);
        ZonedDateTime end        = to.atZone(ZoneOffset.UTC);

        while (!cursor.isAfter(end)) {
            buckets.add(cursor.toInstant());
            cursor = cursor.plus(1, unit);
            if (buckets.size() >= maxBuckets) break;
        }
        return buckets;
    }
}
