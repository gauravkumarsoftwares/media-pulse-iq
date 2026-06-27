package com.java.query.service;

import com.java.query.common.PagedResponse;
import com.java.query.dto.ReconciliationDetailDto;
import com.java.query.dto.ReconciliationStatusEntry;
import com.java.query.dto.ReconciliationSummaryDto;

import java.util.Map;

/**
 * Reconciliation query/trigger service contract (architecture 9.1).
 *
 * <p>Decouples the REST controller from {@link com.java.query.reconciliation.ReconciliationJob}
 * and {@link com.java.query.reconciliation.ReconciliationStore} so each class has a
 * single responsibility and the controller is easily unit-tested by mocking this interface.
 */
public interface ReconciliationQueryService {

    /**
     * Return the latest run summary keyed by window label ({@code hourly}, {@code daily}).
     */
    Map<String, ReconciliationStatusEntry> getStatus();

    /**
     * Return a paginated list of report summaries for the given window (newest-first).
     *
     * @param window {@code hourly} or {@code daily}
     * @param limit  maximum number of results (capped at 48)
     * @throws com.java.query.common.ApiException (400) for unknown window labels
     */
    PagedResponse<ReconciliationSummaryDto> listReports(String window, int limit);

    /**
     * Return the most recent full report for the given window, or {@code null} if no
     * run has been completed yet.
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

