package com.java.processing.operator;

import com.java.model.EventType;
import com.java.model.ShoppingEvent;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * Flink sessionized attribution join operator (architecture 3.2).
 *
 * <p>Keyed by {@code sessionId}. Maintains two pieces of per-session state:
 * <ol>
 *   <li>{@code lastClick} — the most recent {@link EventType#CLICK} event.</li>
 *   <li>{@code attributed} — guard flag that prevents double attribution per session.</li>
 * </ol>
 *
 * <p><strong>Logic:</strong>
 * <ul>
 *   <li>On {@code CLICK}: store the event in state, clear the attribution guard,
 *       register a processing-time timer to expire the click state after the
 *       attribution window.</li>
 *   <li>On {@code ADD_TO_CART}: if a CLICK state is present and the guard is unset,
 *       synthesize a {@link EventType#CLICK_TO_BASKET} event, set the attribution guard,
 *       and emit the conversion.</li>
 *   <li>On timer fire: clear both states (attribution window expired).</li>
 * </ul>
 *
 * <p>State TTL is aligned with the attribution window so that abandoned sessions
 * don't accumulate indefinitely in RocksDB.
 */
public final class AttributionJoinFunction
        extends KeyedProcessFunction<String, ShoppingEvent, ShoppingEvent> {

    private static final long serialVersionUID = 1L;

    private final long attributionWindowHours;

    private transient ValueState<ShoppingEvent> lastClickState;
    private transient ValueState<Boolean> attributedState;

    public AttributionJoinFunction(long attributionWindowHours) {
        this.attributionWindowHours = attributionWindowHours;
    }

    @Override
    public void open(Configuration parameters) {
        long ttlHours = attributionWindowHours + 1L; // slightly longer than window for safety

        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.hours(ttlHours))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupInRocksdbCompactFilter(1_000)
                .build();

        ValueStateDescriptor<ShoppingEvent> clickDescriptor =
                new ValueStateDescriptor<>("last-click", TypeInformation.of(ShoppingEvent.class));
        clickDescriptor.enableTimeToLive(ttlConfig);
        lastClickState = getRuntimeContext().getState(clickDescriptor);

        ValueStateDescriptor<Boolean> attributedDescriptor =
                new ValueStateDescriptor<>("attributed", Boolean.class);
        attributedDescriptor.enableTimeToLive(ttlConfig);
        attributedState = getRuntimeContext().getState(attributedDescriptor);
    }

    @Override
    public void processElement(ShoppingEvent event,
                               Context ctx,
                               Collector<ShoppingEvent> out) throws Exception {

        EventType type = EventType.from(event.getEventType());

        switch (type) {
            case CLICK -> {
                lastClickState.update(event);
                attributedState.clear();
                // Register timer to expire click state after the attribution window.
                long expiryTime = ctx.timerService().currentProcessingTime()
                        + attributionWindowHours * 3_600_000L;
                ctx.timerService().registerProcessingTimeTimer(expiryTime);
            }

            case ADD_TO_CART -> {
                ShoppingEvent click = lastClickState.value();
                if (click == null) {
                    return; // No prior click in this session — not attributable.
                }
                long deltaMs = event.getEventTimestampMs() - click.getEventTimestampMs();
                boolean inWindow = deltaMs >= 0 && deltaMs <= attributionWindowHours * 3_600_000L;

                // attributedState.value() == null means the session is not yet attributed.
                if (inWindow && attributedState.value() == null) {
                    attributedState.update(Boolean.TRUE);
                    out.collect(buildConversion(event, click, deltaMs));
                }
            }

            default -> { /* IMPRESSION, PRODUCT_VIEW, PURCHASE, etc. — no action */ }
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<ShoppingEvent> out) {
        // Attribution window expired — clean up session state.
        lastClickState.clear();
        attributedState.clear();
    }

    // ---- Helpers ----------------------------------------------------------------

    private static ShoppingEvent buildConversion(ShoppingEvent cart,
                                                  ShoppingEvent click,
                                                  long deltaMs) {
        Map<String, String> tags = new HashMap<>();
        tags.put("durationMs", Long.toString(deltaMs));
        tags.put("attributedClickId", click.getEventId());

        return ShoppingEvent.builder()
                .eventId(cart.getEventId() + "_att")
                .tenantId(cart.getTenantId())
                .userId(cart.getUserId())
                .sessionId(cart.getSessionId())
                .campaignId(click.getCampaignId())   // attributed to the click's campaign
                .eventType(EventType.CLICK_TO_BASKET.name())
                .eventTimestampMs(cart.getEventTimestampMs())
                .cost(click.getCost())
                .customTags(tags)
                .build();
    }
}

