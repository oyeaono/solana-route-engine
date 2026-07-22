# solana-route-engine

Research harness for low-latency routing on Solana. Uses a Yellowstone (Geyser) gRPC stream to drive slot-synchronous quoting against a self-hosted Metis (`jupiter-swap-api`) instance, and collects latency / route quality metrics.

```
Yellowstone gRPC --(slots, AMM account updates)--> route-engine --(/quote)--> Metis
                                                       |                  localhost:8080
                                                       +--> latency histograms, route mix,
                                                            slot lag metrics
```

Rust, tokio + tonic. Quote latency is wall-clock around the full HTTP round trip, aggregated in HDR histograms. Route freshness = quote `contextSlot` vs newest processed slot from the Yellowstone stream.

## Prerequisites

- Yellowstone gRPC endpoint + x-token (e.g. Triton One)
- Self-hosted Metis binary, see `scripts/run-metis.sh` for launch flags
- Rust 1.80+

## Setup

```bash
cp config.example.toml config.toml   # fill in endpoints
cargo build --release
```

## Usage

### stream

Subscribes to processed slots via Yellowstone, re-quotes all configured pairs every N slots. Pool account updates of the configured AMM programs (Orca, Raydium) are counted as a liquidity freshness signal. Rolling report every 30s, final report on Ctrl-C.

```bash
./target/release/route-engine stream --every-slots 4
```

```
== route-engine report | latest slot 285113904 | 48211 AMM account updates observed ==
SOL/USDC: 412 ok / 0 err | e2e latency p50 6.84ms p90 9.12ms p99 14.55ms max 21.03ms | avg server routing 2.11ms | avg quote slot lag 1.2
  route mix: Whirlpool 61%, Raydium CLMM 24%, Meteora DLMM 11%
```

When `--max-in-flight` is saturated, new slots are skipped instead of queued, so latency numbers don't include queueing delay.

### bench

Closed-loop load against the local Metis `/quote` endpoint:

```bash
./target/release/route-engine bench --requests 500 --concurrency 8
```

### compare

Same quote sent to the self-hosted Metis instance and a Jupiter-compatible baseline endpoint at the same time; compares latency and `outAmount` (avg delta in bps):

```bash
./target/release/route-engine compare --requests 30 --interval-ms 500
```

## Config

See `config.example.toml`: Yellowstone endpoint/token, AMM programs to watch, Metis base URL, pair list (mints, input amount in base units, slippage bps).
