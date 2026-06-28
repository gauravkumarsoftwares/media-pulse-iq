package com.java.ingestion.dlq;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * Admin REST endpoints for DLQ inspection and event replay (OB-4).
 *
 * <p>All endpoints are under {@code /api/v1/admin/dlq} and are intended for
 * SRE/operator use only. In a production environment these paths must be:
 * <ol>
 *   <li>Protected by network policy (not routed through the public gateway).</li>
 *   <li>Guarded by the existing PASETO authentication filter (the filter runs
 *       on all paths). Operators must hold a valid internal token.</li>
 * </ol>
 *
 * <h2>Endpoints</h2>
 * <pre>
 *  POST /api/v1/admin/dlq/replay   — re-publish DLQ events to the raw topic
 *  GET  /api/v1/admin/dlq/stats    — DLQ partition watermarks and total lag
 * </pre>
 */
@RestController
@RequestMapping("/api/v1/admin/dlq")
@RequiredArgsConstructor
public class DlqReplayController {

    private final DlqReplayService replayService;

    /**
     * Re-publishes DLQ events matching the supplied criteria to the raw topic.
     *
     * <p>Example request:
     * <pre>
     * POST /api/v1/admin/dlq/replay
     * {
     *   "tenantId":  "walmart_us",
     *   "reason":    "kafka-send-failure",
     *   "maxEvents": 500
     * }
     * </pre>
     *
     * @param request filter and limit parameters (all optional)
     * @return summary of replayed / skipped / failed event counts
     */
    @PostMapping("/replay")
    public ResponseEntity<DlqReplaySummary> replay(
            @RequestBody(required = false) DlqReplayRequest request) {
        if (request == null) {
            request = new DlqReplayRequest(null, null, 1000);
        }
        DlqReplaySummary summary = replayService.replay(request);
        return ResponseEntity.ok(summary);
    }

    /**
     * Returns DLQ partition watermarks, per-partition lag, and total lag.
     * No events are consumed by this endpoint.
     *
     * @return map of DLQ diagnostics
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> stats() {
        return ResponseEntity.ok(replayService.getStats());
    }
}

