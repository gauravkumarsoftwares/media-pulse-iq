package com.java.query.service;

import com.java.query.common.ApiException;
import com.java.query.common.PagedResponse;
import com.java.query.dto.ReconciliationDetailDto;
import com.java.query.dto.ReconciliationResultDto;
import com.java.query.dto.ReconciliationRunSummary;
import com.java.query.reconciliation.ReconciliationJob;
import com.java.query.reconciliation.ReconciliationReport;
import com.java.query.reconciliation.ReconciliationResult;
import com.java.query.reconciliation.ReconciliationStore;
import com.java.query.reconciliation.ReconciliationWindow;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Default {@link ReconciliationQueryService} implementation.
 *
 * <p>All mapping from domain objects to API DTOs is centralised here (A2):
 * a single {@link #toRunSummary(ReconciliationReport)} helper replaces the
 * two previously duplicated methods ({@code toStatusEntry} / {@code toSummaryDto}).
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ReconciliationQueryServiceImpl implements ReconciliationQueryService {

    private final ReconciliationJob   reconciliationJob;
    private final ReconciliationStore store;

    // ---- status -------------------------------------------------------------

    @Override
    public Map<String, ReconciliationRunSummary> getStatus() {
        Map<String, ReconciliationRunSummary> result = new LinkedHashMap<>();
        for (ReconciliationWindow w : ReconciliationWindow.values()) {
            Optional<ReconciliationReport> latest = store.findLatest(w);
            result.put(w.getLabel(), latest.map(this::toRunSummary)
                    .orElseGet(ReconciliationQueryServiceImpl::noRunYet));
        }
        return result;
    }

    // ---- list reports -------------------------------------------------------

    @Override
    public PagedResponse<ReconciliationRunSummary> listReports(String window, int limit) {
        ReconciliationWindow w = resolveWindow(window);
        int effectiveLimit = Math.min(Math.max(limit, 1), 48);
        List<ReconciliationRunSummary> summaries = store.findByWindow(w, effectiveLimit)
                .stream().map(this::toRunSummary).toList();
        return PagedResponse.of(summaries);
    }

    // ---- latest report ------------------------------------------------------

    @Override
    public ReconciliationDetailDto getLatestReport(String window) {
        ReconciliationWindow w = resolveWindow(window);
        return store.findLatest(w).map(this::toDetailDto).orElse(null);
    }

    // ---- trigger run --------------------------------------------------------

    @Override
    public ReconciliationDetailDto triggerRun(String window) {
        ReconciliationWindow w = resolveWindow(window);
        log.info("[reconciliation] Manual run triggered via REST for window={}", window);
        ReconciliationReport report = reconciliationJob.runAndStore(w);
        return toDetailDto(report);
    }

    // ---- mapping (A2 — single shared helper) --------------------------------

    /**
     * Maps a {@link ReconciliationReport} to a {@link ReconciliationRunSummary}.
     * Used for both the status endpoint and the paginated list endpoint — the
     * previously duplicated {@code toStatusEntry} and {@code toSummaryDto} methods
     * are replaced by this single method (DRY).
     */
    private ReconciliationRunSummary toRunSummary(ReconciliationReport r) {
        return new ReconciliationRunSummary(
                r.getRunId(),
                r.getStatus().name(),
                r.getRunTime().toString(),
                r.getWindowStart() != null ? r.getWindowStart().toString() : null,
                r.getWindowEnd()   != null ? r.getWindowEnd().toString()   : null,
                r.totalCampaigns(),
                r.discrepancyCount(),
                r.autoPatchedCount(),
                r.getElapsed().toMillis(),
                r.getMessage().isBlank() ? null : r.getMessage());
    }

    private ReconciliationDetailDto toDetailDto(ReconciliationReport r) {
        ReconciliationRunSummary summary = toRunSummary(r);
        List<ReconciliationResultDto> resultDtos =
                r.getResults().stream().map(this::toResultDto).toList();
        return new ReconciliationDetailDto(
                summary.runId(), summary.status(), summary.runTime(),
                summary.windowStart(), summary.windowEnd(),
                summary.campaigns(), summary.discrepancies(), summary.autoPatched(),
                summary.elapsedMs(), summary.message(),
                resultDtos);
    }

    private ReconciliationResultDto toResultDto(ReconciliationResult res) {
        return new ReconciliationResultDto(
                res.key().tenantId(),
                res.key().campaignId(),
                res.key().eventType(),
                res.referenceStore(),
                res.referenceCount(),
                res.observedStore(),
                res.observedCount(),
                res.delta(),
                String.format("%.4f", res.discrepancyPct()),
                res.autoPatched());
    }

    // ---- helpers ------------------------------------------------------------

    private static ReconciliationRunSummary noRunYet() {
        return new ReconciliationRunSummary(
                null, "NO_RUN_YET", null, null, null, 0, 0, 0, 0, null);
    }

    private static ReconciliationWindow resolveWindow(String label) {
        for (ReconciliationWindow w : ReconciliationWindow.values()) {
            if (w.getLabel().equalsIgnoreCase(label)) return w;
        }
        throw new ApiException(HttpStatus.BAD_REQUEST,
                "Unknown window type '" + label + "'. Use 'hourly' or 'daily'.");
    }
}
