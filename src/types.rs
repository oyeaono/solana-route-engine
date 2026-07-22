use serde::Deserialize;

/// Subset of the Jupiter v6 quote response served by the Metis binary.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuoteResponse {
    pub input_mint: String,
    pub in_amount: String,
    pub output_mint: String,
    pub out_amount: String,
    #[serde(default)]
    pub other_amount_threshold: Option<String>,
    #[serde(default)]
    pub price_impact_pct: Option<String>,
    #[serde(default)]
    pub route_plan: Vec<RoutePlanStep>,
    /// Slot the quote was computed against; compared with the live
    /// Yellowstone slot to measure route staleness
    #[serde(default)]
    pub context_slot: Option<u64>,
    /// Server-side routing time in seconds, as reported by the API
    #[serde(default)]
    pub time_taken: Option<f64>,
}

impl QuoteResponse {
    pub fn out_amount_u128(&self) -> Option<u128> {
        self.out_amount.parse().ok()
    }

    pub fn amm_labels(&self) -> impl Iterator<Item = &str> {
        self.route_plan
            .iter()
            .filter_map(|step| step.swap_info.label.as_deref())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RoutePlanStep {
    pub swap_info: SwapInfo,
    #[serde(default)]
    pub percent: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwapInfo {
    pub amm_key: String,
    #[serde(default)]
    pub label: Option<String>,
    pub input_mint: String,
    pub output_mint: String,
    pub in_amount: String,
    pub out_amount: String,
    #[serde(default)]
    pub fee_amount: Option<String>,
    #[serde(default)]
    pub fee_mint: Option<String>,
}
