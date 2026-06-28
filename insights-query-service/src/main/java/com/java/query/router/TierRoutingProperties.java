package com.java.query.router;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Externalised tier-routing boundary configuration (OCP fix — B2).
 *
 * <p>Bound from {@code platform.query.routing.*} in application[-profile].yml.
 * Changing tier boundaries now requires only a config change, not a recompile.
 */
@Component
@ConfigurationProperties(prefix = "platform.query.routing")
@Getter
@Setter
public class TierRoutingProperties {

    /**
     * Queries whose window starts within this many hours ago are served by
     * the Redis hot tier. Default: 48 hours.
     */
    private long hotWindowHours = 48;

    /**
     * Queries whose window starts within this many days ago (but beyond
     * {@link #hotWindowHours}) are served by the Pinot warm tier. Default: 30 days.
     */
    private long warmWindowDays = 30;

    /** @return hot-tier window as a {@link Duration}. */
    public Duration hotWindow()  { return Duration.ofHours(hotWindowHours); }

    /** @return warm-tier window as a {@link Duration}. */
    public Duration warmWindow() { return Duration.ofDays(warmWindowDays); }
}

