mod bench;
mod config;
mod geyser;
mod metis;
mod types;

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tokio::sync::{mpsc, Mutex, Semaphore};

use crate::{
    bench::LatencyRecorder,
    config::{Config, Pair},
    metis::QuoteClient,
};

#[derive(Parser)]
#[command(
    name = "route-engine",
    about = "Low-latency Solana route engine: Yellowstone-driven quoting against a self-hosted Metis (jupiter-swap-api) instance"
)]
struct Cli {
    /// Path to the TOML config file
    #[arg(short, long, default_value = "config.toml")]
    config: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Subscribe to Yellowstone slot updates and re-quote all pairs every N processed slots
    Stream {
        /// Re-quote every N processed slots (a Solana slot is ~400ms)
        #[arg(long, default_value_t = 4)]
        every_slots: u64,
        /// Max quote requests in flight; when saturated, slots are skipped rather than queued
        #[arg(long, default_value_t = 16)]
        max_in_flight: usize,
    },
    /// Fixed-load latency benchmark against the Metis /quote endpoint
    Bench {
        /// Requests per pair
        #[arg(long, default_value_t = 200)]
        requests: usize,
        /// Concurrent requests per pair
        #[arg(long, default_value_t = 8)]
        concurrency: usize,
    },
    /// Compare self-hosted Metis routes and latency against a baseline quote endpoint
    Compare {
        /// Baseline quote API base URL (must expose a Jupiter-compatible /quote)
        #[arg(long, default_value = "https://lite-api.jup.ag/swap/v1")]
        baseline_url: String,
        /// Paired quote iterations per pair
        #[arg(long, default_value_t = 30)]
        requests: usize,
        /// Pause between iterations, to stay under public API rate limits
        #[arg(long, default_value_t = 500)]
        interval_ms: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .init();

    let cli = Cli::parse();
    let cfg = Config::load(&cli.config)
        .with_context(|| format!("failed to load config from {}", cli.config))?;
    anyhow::ensure!(!cfg.pairs.is_empty(), "no [[pairs]] configured in {}", cli.config);

    match cli.command {
        Command::Stream {
            every_slots,
            max_in_flight,
        } => run_stream(cfg, every_slots, max_in_flight).await,
        Command::Bench {
            requests,
            concurrency,
        } => run_bench(cfg, requests, concurrency).await,
        Command::Compare {
            baseline_url,
            requests,
            interval_ms,
        } => run_compare(cfg, baseline_url, requests, interval_ms).await,
    }
}

async fn run_stream(cfg: Config, every_slots: u64, max_in_flight: usize) -> Result<()> {
    let metis = QuoteClient::new(&cfg.metis.base_url)?;
    let latest_slot = Arc::new(AtomicU64::new(0));
    let account_updates = Arc::new(AtomicU64::new(0));
    let (slot_tx, mut slot_rx) = mpsc::channel(1024);

    let geyser_cfg = cfg.yellowstone.clone();
    let geyser_latest = Arc::clone(&latest_slot);
    let geyser_accounts = Arc::clone(&account_updates);
    let geyser_task = tokio::spawn(async move {
        if let Err(err) = geyser::run(geyser_cfg, slot_tx, geyser_latest, geyser_accounts).await {
            tracing::error!("Yellowstone stream failed: {err:#}");
        }
    });

    let recorders: Vec<(Pair, Arc<Mutex<LatencyRecorder>>)> = cfg
        .pairs
        .iter()
        .map(|p| (p.clone(), Arc::new(Mutex::new(LatencyRecorder::new()))))
        .collect();
    let in_flight = Arc::new(Semaphore::new(max_in_flight));

    let mut report_timer = tokio::time::interval(Duration::from_secs(30));
    report_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    report_timer.tick().await; // the first tick fires immediately; consume it

    tracing::info!(
        "streaming: quoting {} pair(s) every {} processed slot(s)",
        recorders.len(),
        every_slots
    );

    let mut last_quoted_slot = 0u64;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = report_timer.tick() => {
                print_stream_report(&recorders, &account_updates, latest_slot.load(Ordering::Relaxed)).await;
            }
            event = slot_rx.recv() => {
                let Some(event) = event else { break };
                if event.slot < last_quoted_slot + every_slots {
                    continue;
                }
                last_quoted_slot = event.slot;
                for (pair, recorder) in &recorders {
                    let Ok(permit) = Arc::clone(&in_flight).try_acquire_owned() else {
                        tracing::warn!(
                            "slot {}: skipping {}, {} quotes already in flight",
                            event.slot, pair.name, max_in_flight
                        );
                        continue;
                    };
                    let metis = metis.clone();
                    let pair = pair.clone();
                    let recorder = Arc::clone(recorder);
                    let latest = Arc::clone(&latest_slot);
                    tokio::spawn(async move {
                        let outcome = metis.quote(&pair).await;
                        let mut rec = recorder.lock().await;
                        match outcome {
                            Ok(timed) => rec.record_ok(
                                timed.latency,
                                &timed.quote,
                                Some(latest.load(Ordering::Relaxed)),
                            ),
                            Err(err) => {
                                rec.record_err();
                                tracing::debug!("quote {} failed: {err:#}", pair.name);
                            }
                        }
                        drop(permit);
                    });
                }
            }
        }
    }

    geyser_task.abort();
    print_stream_report(&recorders, &account_updates, latest_slot.load(Ordering::Relaxed)).await;
    Ok(())
}

async fn print_stream_report(
    recorders: &[(Pair, Arc<Mutex<LatencyRecorder>>)],
    account_updates: &AtomicU64,
    latest_slot: u64,
) {
    println!(
        "\n== route-engine report | latest slot {} | {} AMM account updates observed ==",
        latest_slot,
        account_updates.load(Ordering::Relaxed)
    );
    for (pair, recorder) in recorders {
        println!("{}", recorder.lock().await.report(&pair.name));
    }
}

async fn run_bench(cfg: Config, requests: usize, concurrency: usize) -> Result<()> {
    use futures::StreamExt;

    let metis = QuoteClient::new(&cfg.metis.base_url)?;
    println!(
        "benchmarking {} pair(s) against {} ({} requests per pair, concurrency {})",
        cfg.pairs.len(),
        cfg.metis.base_url,
        requests,
        concurrency
    );

    for pair in &cfg.pairs {
        let recorder = Arc::new(Mutex::new(LatencyRecorder::new()));
        futures::stream::iter(0..requests)
            .for_each_concurrent(concurrency, |_| {
                let metis = metis.clone();
                let recorder = Arc::clone(&recorder);
                async move {
                    match metis.quote(pair).await {
                        Ok(timed) => {
                            recorder.lock().await.record_ok(timed.latency, &timed.quote, None)
                        }
                        Err(err) => {
                            recorder.lock().await.record_err();
                            tracing::debug!("quote {} failed: {err:#}", pair.name);
                        }
                    }
                }
            })
            .await;
        println!("{}", recorder.lock().await.report(&pair.name));
    }
    Ok(())
}

async fn run_compare(
    cfg: Config,
    baseline_url: String,
    requests: usize,
    interval_ms: u64,
) -> Result<()> {
    let metis = QuoteClient::new(&cfg.metis.base_url)?;
    let baseline = QuoteClient::new(&baseline_url)?;
    println!(
        "comparing {} (metis) vs {} (baseline), {} paired iterations per pair",
        cfg.metis.base_url, baseline_url, requests
    );

    for pair in &cfg.pairs {
        let mut metis_rec = LatencyRecorder::new();
        let mut base_rec = LatencyRecorder::new();
        let (mut better, mut equal, mut worse) = (0u64, 0u64, 0u64);
        let mut delta_bps_sum = 0f64;
        let mut delta_samples = 0u64;

        for i in 0..requests {
            let (m, b) = tokio::join!(metis.quote(pair), baseline.quote(pair));

            if let (Ok(mq), Ok(bq)) = (&m, &b) {
                if let (Some(mo), Some(bo)) =
                    (mq.quote.out_amount_u128(), bq.quote.out_amount_u128())
                {
                    match mo.cmp(&bo) {
                        std::cmp::Ordering::Greater => better += 1,
                        std::cmp::Ordering::Equal => equal += 1,
                        std::cmp::Ordering::Less => worse += 1,
                    }
                    if bo > 0 {
                        delta_bps_sum += (mo as f64 - bo as f64) / bo as f64 * 10_000.0;
                        delta_samples += 1;
                    }
                }
            }
            match m {
                Ok(t) => metis_rec.record_ok(t.latency, &t.quote, None),
                Err(err) => {
                    metis_rec.record_err();
                    tracing::debug!("metis quote {} failed: {err:#}", pair.name);
                }
            }
            match b {
                Ok(t) => base_rec.record_ok(t.latency, &t.quote, None),
                Err(err) => {
                    base_rec.record_err();
                    tracing::debug!("baseline quote {} failed: {err:#}", pair.name);
                }
            }
            if i + 1 < requests {
                tokio::time::sleep(Duration::from_millis(interval_ms)).await;
            }
        }

        println!("\n== {} ==", pair.name);
        println!("{}", metis_rec.report("metis    "));
        println!("{}", base_rec.report("baseline "));
        let compared = better + equal + worse;
        if compared > 0 {
            let avg_delta = if delta_samples > 0 {
                delta_bps_sum / delta_samples as f64
            } else {
                0.0
            };
            println!(
                "out-amount vs baseline over {} paired quotes: metis better {} / equal {} / worse {} (avg delta {:+.2} bps)",
                compared, better, equal, worse, avg_delta
            );
        }
    }
    Ok(())
}
