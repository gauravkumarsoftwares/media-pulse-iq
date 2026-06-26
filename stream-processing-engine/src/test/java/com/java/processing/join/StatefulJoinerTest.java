package com.java.processing.join;

import com.java.model.ShoppingEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the sessionized click-to-basket attribution join.
 */
class StatefulJoinerTest {

    private ShoppingEvent click(String session, long ts) {
        return ShoppingEvent.builder()
                .eventId("c-" + ts).tenantId("t").userId("u").sessionId(session)
                .campaignId("camp").eventType("CLICK").eventTimestampMs(ts).cost(0.5).build();
    }

    private ShoppingEvent cart(String session, long ts) {
        return ShoppingEvent.builder()
                .eventId("k-" + ts).tenantId("t").userId("u").sessionId(session)
                .eventType("ADD_TO_CART").eventTimestampMs(ts).build();
    }

    @Test
    @DisplayName("Synthesizes a conversion when cart follows click within window")
    void attributesWithinWindow() {
        StatefulJoiner joiner = new StatefulJoiner(24);
        assertNull(joiner.join(click("s1", 1000)));

        ShoppingEvent conv = joiner.join(cart("s1", 4000));
        assertNotNull(conv);
        assertEquals("CLICK_TO_BASKET", conv.getEventType());
        assertEquals("camp", conv.getCampaignId());
    }

    @Test
    @DisplayName("No conversion when cart exceeds attribution window")
    void rejectsOutsideWindow() {
        StatefulJoiner joiner = new StatefulJoiner(1); // 1 hour window
        joiner.join(click("s2", 0));
        long twoHoursLater = 2 * 60 * 60 * 1000L;
        assertNull(joiner.join(cart("s2", twoHoursLater)));
    }

    @Test
    @DisplayName("Blocks duplicate attribution within the same session")
    void blocksDoubleAttribution() {
        StatefulJoiner joiner = new StatefulJoiner(24);
        joiner.join(click("s3", 1000));
        assertNotNull(joiner.join(cart("s3", 2000)));
        assertNull(joiner.join(cart("s3", 3000)));
    }
}

