package com.java.processing.join;

import com.java.model.EventType;
import com.java.model.ShoppingEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;
import java.util.Map;

/**
 * Sessionized stateful attribution join (architecture §3.2).
 *
 * <p>Keeps the last ad click per session and, when an ADD_TO_CART arrives within
 * the attribution window, synthesizes a CLICK_TO_BASKET event attributed to the
 * click's campaign. A per-session guard prevents double attribution.
 */
@Component
public final class StatefulJoiner {

    private final ConcurrentHashMap<String, ShoppingEvent> lastClickBySession = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> attributedSessions = new ConcurrentHashMap<>();
    private final long attributionWindowMs;

    public StatefulJoiner(@Value("${platform.attribution.window-hours:24}") long windowHours) {
        this.attributionWindowMs = windowHours * 60 * 60 * 1000L;
    }

    /**
     * Process one event and optionally return a synthesized conversion event.
     *
     * @param event a unique (already deduplicated) event
     * @return a CLICK_TO_BASKET event if attribution occurred, else {@code null}
     */
    public ShoppingEvent join(ShoppingEvent event) {
        String sessionId = event.getSessionId();
        EventType type = EventType.from(event.getEventType());

        switch (type) {
            case CLICK -> {
                lastClickBySession.put(sessionId, event);
                attributedSessions.remove(sessionId);
                return null;
            }
            case ADD_TO_CART -> {
                ShoppingEvent click = lastClickBySession.get(sessionId);
                if (click == null) {
                    return null;
                }
                long delta = event.getEventTimestampMs() - click.getEventTimestampMs();
                boolean withinWindow = delta >= 0 && delta <= attributionWindowMs;
                if (withinWindow && attributedSessions.putIfAbsent(sessionId, Boolean.TRUE) == null) {
                    // Time-to-basket conversion speed (architecture §3.2), carried as a tag
                    // since the flattened model has no dedicated duration column.
                    Map<String, String> tags = new HashMap<>();
                    tags.put("durationMs", Long.toString(delta));
                    return ShoppingEvent.builder()
                            .eventId(event.getEventId() + "_att")
                            .tenantId(event.getTenantId())
                            .userId(event.getUserId())
                            .sessionId(sessionId)
                            .campaignId(click.getCampaignId())
                            .eventType(EventType.CLICK_TO_BASKET.name())
                            .eventTimestampMs(event.getEventTimestampMs())
                            .cost(click.getCost())
                            .customTags(tags)
                            .build();
                }
                return null;
            }
            default -> {
                return null;
            }
        }
    }
}
