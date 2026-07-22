use std::{fs, path::Path};

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub yellowstone: YellowstoneConfig,
    pub metis: MetisConfig,
    #[serde(default)]
    pub pairs: Vec<Pair>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct YellowstoneConfig {
    /// Yellowstone gRPC endpoint, e.g. "https://grpc.example.com:443"
    pub endpoint: String,
    #[serde(default)]
    pub x_token: Option<String>,
    /// AMM program ids whose pool accounts are tracked for liquidity freshness
    #[serde(default)]
    pub account_owners: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetisConfig {
    /// Base URL of the self-hosted Metis (jupiter-swap-api) instance
    pub base_url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Pair {
    pub name: String,
    pub input_mint: String,
    pub output_mint: String,
    /// Input amount in base units of the input mint (e.g. lamports for SOL)
    pub amount: u64,
    #[serde(default = "default_slippage_bps")]
    pub slippage_bps: u16,
}

fn default_slippage_bps() -> u16 {
    50
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let raw = fs::read_to_string(path.as_ref())
            .with_context(|| format!("cannot read {}", path.as_ref().display()))?;
        toml::from_str(&raw).context("invalid config file")
    }
}
