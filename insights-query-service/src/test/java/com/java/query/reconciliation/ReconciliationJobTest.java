package com.java.query.reconciliation;

import com.java.query.store.PinotRestClient;
import com.java.query.store.RedisInsightsStore;
import com.java.query.store.TrinoIcebergClient;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for the reconciliation pipeline (strategies + job orchestration).
 *
 * <p>Tests are split per concern:
 * <ul>
 *   <li>{@link HourlyReconciliationStrategy} — Redis vs Pinot comparison logic</li>
 *   <li>{@link DailyReconciliationStrategy}  — Pinot vs Iceberg comparison logic</li>
 *   <li>{@link ReconciliationJob}            — orchestration, store, disabled-flag</li>
 * </ul>
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ReconciliationJobTest {

    @Mock private PinotRestClient    pinotClient;
    @Mock private RedisInsightsStore redisStore;
    @Mock private TrinoIcebergClient trinoClient;

    private ReconciliationJob        job;
    private ReconciliationProperties props;
    private ReconciliationStore      store;
    private ReconciliationMetrics    metrics;

    @BeforeEach
    void setUp() {
        props = new ReconciliationProperties();
        props.setEnabled(true);
        props.setAutoCorrectRedis(true);
        props.setDiscrepancyThresholdPct(1.0);
        props.setMaxCampaignsPerRun(5000);

        store   = new ReconciliationStore();
        metrics = new ReconciliationMetrics(new SimpleMeterRegistry());

        HourlyReconciliationStrategy hourly = new HourlyReconciliationStrategy(
                props, pinotClient, redisStore);
        DailyReconciliationStrategy daily = new DailyReconciliationStrategy(
                props, pinotClient, trinoClient);

        job = new ReconciliationJob(props, List.of(hourly, daily), metrics, store);
    }

    // =========================================================================
    // Hourly job — Redis vs Pinot
    // =========================================================================

    @Test
    @DisplayName("Hourly: OK status when Redis matches Pinot")
    void hourly_noDiscrepancy() {
        CampaignKey key = new CampaignKey("tenant1", "camp1", "CLICK");
        when(pinotClient.queryGroupedCounts(anyLong(), anyLong(), anyInt()))
                .thenReturn(List.of(new CampaignMetricCount(key, 100L)));
        when(redisStore.getCount("tenant1", "camp1", "CLICK"))
                .thenReturn(Optional.of(100L));

        ReconciliationReport report = job.runAndStore(ReconciliationWindow.HOURLY);

        assertThat(report.getStatus()).isEqualTo(ReconciliationReport.RunStatus.OK);
        assertThat(report.totalCampaigns()).isEqualTo(1);
        assertThat(report.discrepancyCount()).isEqualTo(0);
        assertThat(report.autoPatchedCount()).isEqualTo(0);
        verify(redisStore, never()).incrementCount(any(), any(), any(), anyLong());
    }

    @Test
    @DisplayName("Hourly: auto-patches Redis when Redis count is below Pinot")
    void hourly_autoPatches_redisUnderCount() {
        CampaignKey key = new CampaignKey("tenant1", "camp1", "CLICK");
        when(pinotClient.queryGroupedCounts(anyLong(), anyLong(), anyInt()))
                .thenReturn(List.of(new CampaignMetricCount(key, 150L)));
        when(redisStore.getCount("tenant1", "camp1", "CLICK"))
                .thenReturn(Optional.of(100L));

        ReconciliationReport report = job.runAndStore(ReconciliationWindow.HOURLY);

        assertThat(report.autoPatchedCount()).isEqualTo(1);
        verify(redisStore).incrementCount("tenant1", "camp1", "CLICK", 50L);
    }

    @Test
    @DisplayName("Hourly: does NOT patch Redis when Redis count is above Pinot")
    void hourly_noAutoPatch_redisOverCount() {
        CampaignKey key = new CampaignKey("tenant1", "camp1", "CLICK");
        when(pinotClient.queryGroupedCounts(anyLong(), anyLong(), anyInt()))
                .thenReturn(List.of(new CampaignMetricCount(key, 100L)));
        when(redisStore.getCount("tenant1", "camp1", "CLICK"))
                .thenReturn(Optional.of(120L));

        ReconciliationReport report = job.runAndStore(ReconciliationWindow.HOURLY);

        assertThat(report.autoPatchedCount()).isEqualTo(0);
        verify(redisStore, never()).incrementCount(any(), any(), any(), anyLong());
        assertThat(report.getResults().get(0).delta()).isEqualTo(20L);
    }

    @Test
    @DisplayName("Hourly: treats missing Redis key (empty) as zero and patches")
    void hourly_autoPatches_missingRedisKey() {
        CampaignKey key = new CampaignKey("tenant2", "camp2", "IMPRESSION");
        when(pinotClient.queryGroupedCounts(anyLong(), anyLong(), anyInt()))
                .thenReturn(List.of(new CampaignMetricCount(key, 500L)));
        when(redisStore.getCount("tenant2", "camp2", "IMPRESSION"))
                .thenReturn(Optional.empty());

        ReconciliationReport report = job.runAndStore(ReconciliationWindow.HOURLY);

        assertThat(report.autoPatchedCount()).isEqualTo(1);
        verify(redisStore).incrementCount("tenant2", "camp2", "IMPRESSION", 500L);
    }

    @Test
    @DisplayName("Hourly: skips auto-patch when auto-correct-redis is disabled")
    void hourly_respectsAutoCorrectDisabledFlag() {
        props.setAutoCorrectRedis(false);
        CampaignKey key = new CampaignKey("tenant1", "camp1", "CLICK");
        when(pinotClient.queryGroupedCounts(anyLong(), anyLong(), anyInt()))
                .thenReturn(List.of(new CampaignMetricCount(key, 100L)));
        when(redisStore.getCount("tenant1", "camp1", "CLICK"))
                .thenReturn(Optional.of(10L));

        ReconciliationReport report = job.runAndStore(ReconciliationWindow.HOURLY);

        assertThat(report.autoPatchedCount()).isEqualTo(0);
        verify(redisStore, never()).incrementCount(any(), any(), any(), anyLong());
    }

    @Test
    @DisplayName("Hourly: empty Pinot result produces OK report with zero campaigns")
    void hourly_emptyPinot() {
        when(pinotClient.queryGroupedCounts(anyLong(), anyLong(), anyInt()))
                .thenReturn(List.of());

        ReconciliationReport report = job.runAndStore(ReconciliationWindow.HOURLY);

        assertThat(report.getStatus()).isEqualTo(ReconciliationReport.RunStatus.OK);
        assertThat(report.totalCampaigns()).isEqualTo(0);
    }

    // =========================================================================
    // Daily job — Pinot vs Iceberg
    // =========================================================================

    @Test
    @DisplayName("Daily: OK status when Pinot and Iceberg counts match")
    void daily_noDiscrepancy() {
        CampaignKey key = new CampaignKey("tenant1", "camp1", "CLICK");
        when(pinotClient.queryGroupedCounts(anyLong(), anyLong(), anyInt()))
                .thenReturn(List.of(new CampaignMetricCount(key, 1000L)));
        when(trinoClient.queryGroupedCounts(anyLong(), anyLong(), anyInt()))
                .thenReturn(List.of(new CampaignMetricCount(key, 1000L)));

        ReconciliationReport report = job.runAndStore(ReconciliationWindow.DAILY);

        assertThat(report.getStatus()).isEqualTo(ReconciliationReport.RunStatus.OK);
        assertThat(report.discrepancyCount()).isEqualTo(0);
    }

    @Test
    @DisplayName("Daily: DISCREPANCIES_FOUND when Pinot and Iceberg diverge beyond threshold")
    void daily_discrepancyFlaggedAboveThreshold() {
        CampaignKey key = new CampaignKey("tenant1", "camp1", "CLICK");
        when(pinotClient.queryGroupedCounts(anyLong(), anyLong(), anyInt()))
                .thenReturn(List.of(new CampaignMetricCount(key, 950L)));
        when(trinoClient.queryGroupedCounts(anyLong(), anyLong(), anyInt()))
                .thenReturn(List.of(new CampaignMetricCount(key, 1000L)));

        ReconciliationReport report = job.runAndStore(ReconciliationWindow.DAILY);

        assertThat(report.getStatus()).isEqualTo(ReconciliationReport.RunStatus.DISCREPANCIES_FOUND);
        assertThat(report.discrepancyCount()).isEqualTo(1);
        ReconciliationResult result = report.getResults().get(0);
        assertThat(result.delta()).isEqualTo(-50L);
        assertThat(result.discrepancyPct()).isGreaterThan(1.0);
    }

    @Test
    @DisplayName("Daily: skips comparison when Trino returns 0 (stub mode)")
    void daily_skipsWhenTrinoReturnsZero() {
        CampaignKey key = new CampaignKey("tenant1", "camp1", "CLICK");
        when(pinotClient.queryGroupedCounts(anyLong(), anyLong(), anyInt()))
                .thenReturn(List.of(new CampaignMetricCount(key, 500L)));
        when(trinoClient.queryGroupedCounts(anyLong(), anyLong(), anyInt()))
                .thenReturn(List.of());

        ReconciliationReport report = job.runAndStore(ReconciliationWindow.DAILY);

        assertThat(report.totalCampaigns()).isEqualTo(0);
        assertThat(report.getStatus()).isEqualTo(ReconciliationReport.RunStatus.OK);
    }

    // =========================================================================
    // ReconciliationStore
    // =========================================================================

    @Test
    @DisplayName("Store saves reports and returns them newest-first")
    void store_savesAndReturnsNewestFirst() {
        when(pinotClient.queryGroupedCounts(anyLong(), anyLong(), anyInt()))
                .thenReturn(List.of());

        ReconciliationReport r1 = job.runAndStore(ReconciliationWindow.HOURLY);
        ReconciliationReport r2 = job.runAndStore(ReconciliationWindow.HOURLY);

        List<ReconciliationReport> reports = store.findByWindow(ReconciliationWindow.HOURLY, 10);
        assertThat(reports).hasSize(2);
        assertThat(reports.get(0).getRunId()).isEqualTo(r2.getRunId()); // newest first
        assertThat(reports.get(1).getRunId()).isEqualTo(r1.getRunId());
    }

    @Test
    @DisplayName("Store caps at MAX_REPORTS_PER_WINDOW entries")
    void store_evictsOldestWhenFull() {
        when(pinotClient.queryGroupedCounts(anyLong(), anyLong(), anyInt()))
                .thenReturn(List.of());

        for (int i = 0; i <= ReconciliationStore.MAX_REPORTS_PER_WINDOW + 2; i++) {
            job.runAndStore(ReconciliationWindow.HOURLY);
        }

        List<ReconciliationReport> reports = store.findByWindow(
                ReconciliationWindow.HOURLY, Integer.MAX_VALUE);
        assertThat(reports).hasSize(ReconciliationStore.MAX_REPORTS_PER_WINDOW);
    }

    // =========================================================================
    // Disabled feature toggle
    // =========================================================================

    @Test
    @DisplayName("runHourly is no-op when reconciliation is disabled")
    void hourly_disabledByFlag() {
        props.setEnabled(false);
        job.runHourly();
        verify(pinotClient, never()).queryGroupedCounts(anyLong(), anyLong(), anyInt());
    }

    @Test
    @DisplayName("runDaily is no-op when reconciliation is disabled")
    void daily_disabledByFlag() {
        props.setEnabled(false);
        job.runDaily();
        verify(pinotClient, never()).queryGroupedCounts(anyLong(), anyLong(), anyInt());
    }
}
