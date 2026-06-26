package com.java.query.common;

import java.util.List;

/**
 * Generic paginated response envelope (API Guideline §3).
 *
 * @param <T>       item type
 * @param items     the result page
 * @param total     total items available (across all pages)
 * @param page      current 0-based page index
 * @param pageSize  items per page
 * @param totalPages total number of pages
 */
public record PagedResponse<T>(
        List<T> items,
        long total,
        int page,
        int pageSize,
        int totalPages) {

    /** Convenience factory for limit-based (non-paginated) responses. */
    public static <T> PagedResponse<T> of(List<T> items) {
        return new PagedResponse<>(items, items.size(), 0, items.size(), 1);
    }
}

