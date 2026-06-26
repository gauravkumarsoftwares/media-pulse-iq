package com.java.query.reconciliation;

import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * In-memory ring-buffer that retains the last N {@link ReconciliationReport}s for
 * each {@link ReconciliationWindow} type.
 *
 * <p>Thread-safe via synchronization on per-window deques.  Reports are discarded
 * when the capacity limit is exceeded (oldest evicted first).
 */
@Component
public class ReconciliationStore {

    /** Maximum reports retained per window type. */
    static final int MAX_REPORTS_PER_WINDOW = 48; // 48 h of hourly reports, or 48 daily reports

    private final Map<ReconciliationWindow, Deque<ReconciliationReport>> store;

    public ReconciliationStore() {
        store = new EnumMap<>(ReconciliationWindow.class);
        for (ReconciliationWindow w : ReconciliationWindow.values()) {
            store.put(w, new ArrayDeque<>(MAX_REPORTS_PER_WINDOW + 1));
        }
    }

    /**
     * Store a completed report.  If the ring-buffer is full, the oldest report
     * is silently evicted.
     */
    public synchronized void save(ReconciliationReport report) {
        Deque<ReconciliationReport> deque = store.get(report.getWindow());
        deque.addLast(report);
        if (deque.size() > MAX_REPORTS_PER_WINDOW) {
            deque.removeFirst();
        }
    }

    /**
     * Return all stored reports for the given window, newest-first.
     *
     * @param window  the reconciliation window type
     * @param limit   maximum number of reports to return; {@code -1} = all
     */
    public synchronized List<ReconciliationReport> findByWindow(
            ReconciliationWindow window, int limit) {
        Deque<ReconciliationReport> deque = store.get(window);
        List<ReconciliationReport> all = new ArrayList<>(deque);
        // Reverse to newest-first
        List<ReconciliationReport> reversed = new ArrayList<>(all.size());
        for (int i = all.size() - 1; i >= 0; i--) {
            reversed.add(all.get(i));
            if (limit > 0 && reversed.size() >= limit) break;
        }
        return reversed;
    }

    /**
     * Return the most recent report for the given window, if any.
     */
    public synchronized Optional<ReconciliationReport> findLatest(ReconciliationWindow window) {
        Deque<ReconciliationReport> deque = store.get(window);
        return deque.isEmpty() ? Optional.empty() : Optional.of(deque.peekLast());
    }
}

