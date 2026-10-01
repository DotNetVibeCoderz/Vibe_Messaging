//! Lock-free broker metrics rendered as OpenMetrics text (port 9645) and JSON (admin API).

use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use serde::Serialize;

/// Latency buckets in seconds (upper bounds).
const BUCKETS: [f64; 14] = [0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];

#[derive(Default)]
pub struct Histogram {
    buckets: [AtomicU64; 14],
    count: AtomicU64,
    sum_micros: AtomicU64,
}

impl Histogram {
    pub fn observe(&self, d: Duration) {
        let s = d.as_secs_f64();
        for (i, b) in BUCKETS.iter().enumerate() {
            if s <= *b {
                self.buckets[i].fetch_add(1, Relaxed);
            }
        }
        self.count.fetch_add(1, Relaxed);
        self.sum_micros.fetch_add(d.as_micros() as u64, Relaxed);
    }

    /// Approximate quantile from the cumulative buckets.
    pub fn quantile(&self, q: f64) -> f64 {
        let total = self.count.load(Relaxed);
        if total == 0 {
            return 0.0;
        }
        let target = (total as f64 * q).ceil() as u64;
        for (i, b) in BUCKETS.iter().enumerate() {
            if self.buckets[i].load(Relaxed) >= target {
                return *b;
            }
        }
        f64::INFINITY
    }

    fn render(&self, out: &mut String, name: &str, help: &str) {
        let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} histogram");
        for (i, b) in BUCKETS.iter().enumerate() {
            let _ = writeln!(out, "{name}_bucket{{le=\"{b}\"}} {}", self.buckets[i].load(Relaxed));
        }
        let count = self.count.load(Relaxed);
        let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {count}");
        let _ = writeln!(out, "{name}_sum {}", self.sum_micros.load(Relaxed) as f64 / 1e6);
        let _ = writeln!(out, "{name}_count {count}");
    }
}

#[derive(Default)]
pub struct Metrics {
    pub connections_open: AtomicU64,
    pub connections_total: AtomicU64,
    pub requests_total: AtomicU64,
    pub request_errors_total: AtomicU64,
    pub produce_records_total: AtomicU64,
    pub produce_bytes_total: AtomicU64,
    pub fetch_bytes_total: AtomicU64,
    pub fetch_records_total: AtomicU64,
    pub http_requests_total: AtomicU64,
    pub diskless_files_total: AtomicU64,
    pub diskless_bytes_total: AtomicU64,
    pub tiered_uploads_total: AtomicU64,
    pub compactions_total: AtomicU64,
    pub compaction_removed_records_total: AtomicU64,
    pub rebalances_total: AtomicU64,
    pub produce_latency: Histogram,
    pub fetch_latency: Histogram,
    pub diskless_put_latency: Histogram,
}

#[derive(Serialize)]
pub struct MetricsSnapshot {
    pub connections_open: u64,
    pub requests_total: u64,
    pub request_errors_total: u64,
    pub produce_records_total: u64,
    pub produce_bytes_total: u64,
    pub fetch_bytes_total: u64,
    pub http_requests_total: u64,
    pub diskless_files_total: u64,
    pub diskless_bytes_total: u64,
    pub tiered_uploads_total: u64,
    pub compactions_total: u64,
    pub rebalances_total: u64,
    pub produce_p50_ms: f64,
    pub produce_p99_ms: f64,
    pub fetch_p99_ms: f64,
    pub diskless_put_p99_ms: f64,
}

impl Metrics {
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            connections_open: self.connections_open.load(Relaxed),
            requests_total: self.requests_total.load(Relaxed),
            request_errors_total: self.request_errors_total.load(Relaxed),
            produce_records_total: self.produce_records_total.load(Relaxed),
            produce_bytes_total: self.produce_bytes_total.load(Relaxed),
            fetch_bytes_total: self.fetch_bytes_total.load(Relaxed),
            http_requests_total: self.http_requests_total.load(Relaxed),
            diskless_files_total: self.diskless_files_total.load(Relaxed),
            diskless_bytes_total: self.diskless_bytes_total.load(Relaxed),
            tiered_uploads_total: self.tiered_uploads_total.load(Relaxed),
            compactions_total: self.compactions_total.load(Relaxed),
            rebalances_total: self.rebalances_total.load(Relaxed),
            produce_p50_ms: self.produce_latency.quantile(0.5) * 1000.0,
            produce_p99_ms: self.produce_latency.quantile(0.99) * 1000.0,
            fetch_p99_ms: self.fetch_latency.quantile(0.99) * 1000.0,
            diskless_put_p99_ms: self.diskless_put_latency.quantile(0.99) * 1000.0,
        }
    }

    pub fn render_openmetrics(&self, extra: &str) -> String {
        let mut out = String::with_capacity(4096);
        let counters: [(&str, &str, &AtomicU64); 14] = [
            ("bp_connections_total", "Kafka connections accepted", &self.connections_total),
            ("bp_requests_total", "Kafka requests handled", &self.requests_total),
            ("bp_request_errors_total", "Kafka requests that failed to decode", &self.request_errors_total),
            ("bp_produce_records_total", "Records appended", &self.produce_records_total),
            ("bp_produce_bytes_total", "Bytes appended", &self.produce_bytes_total),
            ("bp_fetch_bytes_total", "Bytes served to consumers", &self.fetch_bytes_total),
            ("bp_fetch_records_total", "Records served over HTTP", &self.fetch_records_total),
            ("bp_http_requests_total", "HTTP gateway requests", &self.http_requests_total),
            ("bp_diskless_files_total", "Diskless objects written", &self.diskless_files_total),
            ("bp_diskless_bytes_total", "Diskless bytes written", &self.diskless_bytes_total),
            ("bp_tiered_uploads_total", "Segments uploaded to object storage", &self.tiered_uploads_total),
            ("bp_group_rebalances_total", "Consumer group rebalances", &self.rebalances_total),
            ("bp_compactions_total", "Partition compactions completed", &self.compactions_total),
            ("bp_compaction_removed_records_total", "Records removed by log compaction", &self.compaction_removed_records_total),
        ];
        for (name, help, v) in counters {
            let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} counter\n{name} {}", v.load(Relaxed));
        }
        let _ = writeln!(
            out,
            "# HELP bp_connections_open Open Kafka connections\n# TYPE bp_connections_open gauge\nbp_connections_open {}",
            self.connections_open.load(Relaxed)
        );
        self.produce_latency.render(&mut out, "bp_produce_latency_seconds", "Produce latency (request to ack)");
        self.fetch_latency.render(&mut out, "bp_fetch_latency_seconds", "Fetch latency excluding long-poll wait");
        self.diskless_put_latency.render(&mut out, "bp_diskless_put_latency_seconds", "Diskless object PUT latency");
        out.push_str(extra);
        out.push_str("# EOF\n");
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantiles() {
        let h = Histogram::default();
        for _ in 0..99 {
            h.observe(Duration::from_micros(300));
        }
        h.observe(Duration::from_millis(40));
        assert_eq!(h.quantile(0.5), 0.0005);
        assert_eq!(h.quantile(1.0), 0.05);
    }
}
