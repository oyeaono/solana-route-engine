use std::{collections::HashMap, fmt::Write as _, time::Duration};

use hdrhistogram::Histogram;

use crate::types::QuoteResponse;

/// Aggregates quote latency, route composition and freshness metrics.
pub struct LatencyRecorder {
    hist: Histogram<u64>, // microseconds
    ok: u64,
    errors: u64,
    slot_lag_sum: u128,
    slot_lag_samples: u64,
    server_time_sum: f64,
    server_time_samples: u64,
    amm_labels: HashMap<String, u64>,
}

impl LatencyRecorder {
    pub fn new() -> Self {
        Self {
            hist: Histogram::new_with_bounds(1, 60_000_000, 3).expect("static histogram bounds"),
            ok: 0,
            errors: 0,
            slot_lag_sum: 0,
            slot_lag_samples: 0,
            server_time_sum: 0.0,
            server_time_samples: 0,
            amm_labels: HashMap::new(),
        }
    }

    pub fn record_ok(&mut self, latency: Duration, quote: &QuoteResponse, latest_slot: Option<u64>) {
        self.ok += 1;
        let micros = latency.as_micros().clamp(1, 60_000_000) as u64;
        let _ = self.hist.record(micros);
        for label in quote.amm_labels() {
            *self.amm_labels.entry(label.to_string()).or_insert(0) += 1;
        }
        if let (Some(ctx), Some(latest)) = (quote.context_slot, latest_slot) {
            if latest >= ctx {
                self.slot_lag_sum += u128::from(latest - ctx);
                self.slot_lag_samples += 1;
            }
        }
        if let Some(t) = quote.time_taken {
            self.server_time_sum += t;
            self.server_time_samples += 1;
        }
    }

    pub fn record_err(&mut self) {
        self.errors += 1;
    }

    pub fn report(&self, name: &str) -> String {
        let mut out = String::new();
        if self.ok == 0 {
            let _ = write!(out, "{name}: no successful quotes yet ({} errors)", self.errors);
            return out;
        }
        let _ = write!(
            out,
            "{name}: {} ok / {} err | e2e latency p50 {} p90 {} p99 {} max {}",
            self.ok,
            self.errors,
            fmt_ms(self.hist.value_at_quantile(0.50)),
            fmt_ms(self.hist.value_at_quantile(0.90)),
            fmt_ms(self.hist.value_at_quantile(0.99)),
            fmt_ms(self.hist.max()),
        );
        if self.server_time_samples > 0 {
            let _ = write!(
                out,
                " | avg server routing {:.2}ms",
                self.server_time_sum / self.server_time_samples as f64 * 1000.0
            );
        }
        if self.slot_lag_samples > 0 {
            let _ = write!(
                out,
                " | avg quote slot lag {:.1}",
                self.slot_lag_sum as f64 / self.slot_lag_samples as f64
            );
        }
        if !self.amm_labels.is_empty() {
            let mut labels: Vec<_> = self.amm_labels.iter().collect();
            labels.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
            let top: Vec<String> = labels
                .iter()
                .take(5)
                .map(|(label, count)| {
                    format!("{label} {:.0}%", **count as f64 / self.ok as f64 * 100.0)
                })
                .collect();
            let _ = write!(out, "\n  route mix: {}", top.join(", "));
        }
        out
    }
}

fn fmt_ms(micros: u64) -> String {
    format!("{:.2}ms", micros as f64 / 1000.0)
}
