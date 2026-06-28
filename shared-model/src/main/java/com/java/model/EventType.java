package com.java.model;

/**
 * Enumerates the canonical event types tracked across the shopping network,
 * aligned with the Protobuf schema in SDD.md (2.1).
 */
public enum EventType {
    UNSPECIFIED,
    IMPRESSION,
    CLICK,
    PRODUCT_VIEW,
    ADD_TO_CART,
    PURCHASE,
    /** Synthesized by the stream engine when an ADD_TO_CART is attributed to a prior CLICK. */
    CLICK_TO_BASKET;

    /**
     * Null-safe, case-insensitive parser. Unknown values map to {@link #UNSPECIFIED}.
     *
     * @param raw the raw event type string
     * @return the matching {@link EventType}, or {@link #UNSPECIFIED}
     */
    public static EventType from(String raw) {
        if (raw == null) {
            return UNSPECIFIED;
        }
        try {
            return EventType.valueOf(raw.trim().toUpperCase());
        } catch (IllegalArgumentException ex) {
            return UNSPECIFIED;
        }
    }
}

