package com.java.query.service;

import com.java.query.common.PagedResponse;
import com.java.query.dto.ReconciliationDetailDto;
import com.java.query.dto.ReconciliationRunSummary;

import java.util.Map;

/**
 * Reconciliation query/trigger service contract (architecture 9.1).
 *
 * <p>Uses {@link ReconciliationRunSummary} for both the status endpoint and
 * the paginated list endpoint, eliminating the duplicate
 * {@code ReconciliationStatusEntry} / {@code ReconciliationSummaryDto} records (A1).
 */
public interface ReconciliationQueryService {

    /** Latest run summary keyed by window label ({@code hourly}, {@code daily}). */
    Map<String, ReconciliationRunSummary> getStatus();

    /**
     * Paginated list of report summaries for the given window (newest-first).
     *
     * @throws com.java.query.common.ApiException (400) for unknown window labels
     */
    PagedResponse<ReconciliationRunSummary> listReports(String window, int limit);

    /**
     * Most recent full report for the given window, or {@code null} if no run yet.
     *
     * @throws com.java.query.common.ApiException (400) for unknown window labels
     */
    ReconciliationDetailDto getLatestReport(String window);

    /**
     * Trigger an on-demand reconciliation run and return the completed report.
     *
     * @throws com.java.query.common.ApiException (400) for unknown window labels
     */
    ReconciliationDetailDto triggerRun(String window);
}
