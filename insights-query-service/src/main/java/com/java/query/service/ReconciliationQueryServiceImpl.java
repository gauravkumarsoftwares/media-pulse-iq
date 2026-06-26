package com.java.query.service;

import com.java.query.common.ApiException;
import com.java.query.common.PagedResponse;
import com.java.query.dto.ReconciliationDetailDto;
import com.java.query.dto.ReconciliationResultDto;
import com.java.query.dto.ReconciliationStatusEntry;
import com.java.query.dto.ReconciliationSummaryDto;
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
 * <p>All mapping from internal domain objects ({@link ReconciliationReport},
 * {@link ReconciliationResult}) to API DTOs is centralized here, keeping both
 * the controller and the domain model free of serialization concerns.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ReconciliationQueryServiceImpl implements ReconciliationQueryService {

    private final ReconciliationJob   reconciliationJob;
    private final ReconciliationStore store;

    // ---- status -------------------------------------------------------------

    @Override
    public Map<String, ReconciliationStatusEntry> getStatus() {
        Map<String, ReconciliationStatusEntry> result = new LinkedHashMap<>();
        for (ReconciliationWindow w : ReconciliationWindow.values()) {
            Optional<ReconciliationReport> latest = store.findLatest(w);
            result.put(w.getLabel(), latest.map(this::toStatusEntry)
                    .orElseGet(ReconciliationQueryServiceImpl::noRunYet));
        }
        return result;
    }

    // ---- list reports -------------------------------------------------------

    @Override
    public PagedResponse<ReconciliationSummaryDto> listReports(String window, int limit) {
        ReconciliationWindow w = resolveWindow(window);
        int effectiveLimit = Math.min(Math.max(limit, 1), 48);
        List<ReconciliationSummaryDto> summaries = store.findByWindow(w, effectiveLimit)
                .stream().map(this::toSummaryDto).toList();
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

    // ---- window resolution --------------------------------------------------

    private static ReconciliationWindow resolveWindow(String label) {
        for (ReconciliationWindow w : ReconciliationWindow.values()) {
            if (w.getLabel().equalsIgnoreCase(label)) return w;
        }
        throw new ApiException(HttpStatus.BAD_REQUEST,
                "Unknown window type '" + label + "'. Use 'hourly' or 'daily'.");
    }

    // ---- mapping ------------------------------------------------------------

    private ReconciliationStatusEntry toStatusEntry(ReconciliationReport r) {
        return new ReconciliationStatusEntry(
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

    private static ReconciliationStatusEntry noRunYet() {
        return new ReconciliationStatusEntry(
                null, "NO_RUN_YET", null, null, null, 0, 0, 0, 0, null);
    }

    private ReconciliationSummaryDto toSummaryDto(ReconciliationReport r) {
        return new ReconciliationSummaryDto(
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
        List<ReconciliationResultDto> resultDtos = r.getResults().stream()
                .map(this::toResultDto).toList();
        return new ReconciliationDetailDto(
                r.getRunId(),
                r.getStatus().name(),
                r.getRunTime().toString(),
                r.getWindowStart() != null ? r.getWindowStart().toString() : null,
                r.getWindowEnd()   != null ? r.getWindowEnd().toString()   : null,
                r.totalCampaigns(),
                r.discrepancyCount(),
                r.autoPatchedCount(),
                r.getElapsed().toMillis(),
                r.getMessage().isBlank() ? null : r.getMessage(),
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
}


