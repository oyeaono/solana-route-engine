use std::time::{Duration, Instant};

use anyhow::{Context, Result};

use crate::{config::Pair, types::QuoteResponse};

/// Thin HTTP client for a Jupiter-compatible /quote endpoint
/// (self-hosted Metis binary or a hosted baseline).
#[derive(Clone)]
pub struct QuoteClient {
    http: reqwest::Client,
    quote_url: String,
}

pub struct TimedQuote {
    pub latency: Duration,
    pub quote: QuoteResponse,
}

impl QuoteClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .pool_idle_timeout(None) // keep connections warm between slots
            .tcp_nodelay(true)
            .build()
            .context("failed to build HTTP client")?;
        Ok(Self {
            http,
            quote_url: format!("{}/quote", base_url.trim_end_matches('/')),
        })
    }

    /// GET /quote, measuring wall-clock latency of the full round trip.
    pub async fn quote(&self, pair: &Pair) -> Result<TimedQuote> {
        let amount = pair.amount.to_string();
        let slippage = pair.slippage_bps.to_string();
        let query = [
            ("inputMint", pair.input_mint.as_str()),
            ("outputMint", pair.output_mint.as_str()),
            ("amount", amount.as_str()),
            ("slippageBps", slippage.as_str()),
        ];

        let started = Instant::now();
        let response = self
            .http
            .get(&self.quote_url)
            .query(&query)
            .send()
            .await
            .context("quote request failed")?;
        let status = response.status();
        let body = response.bytes().await.context("failed to read quote body")?;
        let latency = started.elapsed();

        anyhow::ensure!(
            status.is_success(),
            "quote returned {status}: {}",
            String::from_utf8_lossy(&body)
        );
        let quote: QuoteResponse =
            serde_json::from_slice(&body).context("failed to parse quote response")?;
        Ok(TimedQuote { latency, quote })
    }
}
