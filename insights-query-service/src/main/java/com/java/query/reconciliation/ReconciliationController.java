package com.java.query.reconciliation;

import com.java.query.common.PagedResponse;
import com.java.query.dto.ReconciliationDetailDto;
import com.java.query.dto.ReconciliationStatusEntry;
import com.java.query.dto.ReconciliationSummaryDto;
import com.java.query.service.ReconciliationQueryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST API for the reconciliation sub-system (architecture §9.1).
 *
 * <pre>
 *   GET  /api/v1/reconciliation/status                  — latest run summary per window
 *   GET  /api/v1/reconciliation/reports/{window}        — paginated report list
 *   GET  /api/v1/reconciliation/reports/{window}/latest — most recent full report
 *   POST /api/v1/reconciliation/runs/{window}           — trigger an on-demand run
 * </pre>
 *
 * <p>All mutation endpoints ({@code POST /runs}) are intended for SRE tooling
 * and should be protected behind an internal network or an admin role in production.
 */
@RestController
@RequestMapping("/api/v1/reconciliation")
@RequiredArgsConstructor
@Tag(name = "Reconciliation", description = "Data consistency reconciliation sub-system")
public class ReconciliationController {

    private final ReconciliationQueryService reconciliationService;

    // ---- GET /status -------------------------------------------------------

    @GetMapping("/status")
    @Operation(summary = "Latest run summary for all reconciliation windows")
    @ApiResponse(responseCode = "200", description = "Status map keyed by window label")
    public ResponseEntity<Map<String, ReconciliationStatusEntry>> status() {
        return ResponseEntity.ok(reconciliationService.getStatus());
    }

    // ---- GET /reports/{window} ---------------------------------------------

    @GetMapping("/reports/{window}")
    @Operation(summary = "Paginated report list for a window type (newest first)")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Report list returned"),
            @ApiResponse(responseCode = "400", description = "Unknown window type")
    })
    public ResponseEntity<PagedResponse<ReconciliationSummaryDto>> listReports(
            @PathVariable String window,
            @RequestParam(defaultValue = "10") int limit) {
        return ResponseEntity.ok(reconciliationService.listReports(window, limit));
    }

    // ---- GET /reports/{window}/latest -------------------------------------

    @GetMapping("/reports/{window}/latest")
    @Operation(summary = "Most recent full report for a window type")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Report returned (or NO_RUN_YET)"),
            @ApiResponse(responseCode = "400", description = "Unknown window type")
    })
    public ResponseEntity<?> latestReport(@PathVariable String window) {
        ReconciliationDetailDto detail = reconciliationService.getLatestReport(window);
        if (detail == null) {
            return ResponseEntity.ok(Map.of("window", window, "status", "NO_RUN_YET"));
        }
        return ResponseEntity.ok(detail);
    }

    // ---- POST /runs/{window} -----------------------------------------------

    @PostMapping("/runs/{window}")
    @Operation(summary = "Trigger an on-demand reconciliation run",
               description = "Runs synchronously. Protect with an admin role in production.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Run completed; full report returned"),
            @ApiResponse(responseCode = "400", description = "Unknown window type")
    })
    public ResponseEntity<ReconciliationDetailDto> triggerRun(@PathVariable String window) {
        ReconciliationDetailDto report = reconciliationService.triggerRun(window);
        return ResponseEntity.status(HttpStatus.OK).body(report);
    }
}
