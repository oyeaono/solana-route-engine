use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::Result;
use futures::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, CommitmentLevel, SlotStatus, SubscribeRequest,
    SubscribeRequestFilterAccounts, SubscribeRequestFilterSlots, SubscribeRequestPing,
};

use crate::config::YellowstoneConfig;

pub struct SlotEvent {
    pub slot: u64,
    pub received_at: Instant,
}

/// Connects to Yellowstone gRPC and forwards processed-slot events into
/// `slot_tx`. Also tracks the newest slot seen and, if `account_owners` is
/// configured, counts AMM pool account updates as a liquidity-freshness signal.
pub async fn run(
    cfg: YellowstoneConfig,
    slot_tx: mpsc::Sender<SlotEvent>,
    latest_slot: Arc<AtomicU64>,
    account_updates: Arc<AtomicU64>,
) -> Result<()> {
    let mut client = GeyserGrpcClient::build_from_shared(cfg.endpoint.clone())?
        .x_token(cfg.x_token.clone())?
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .connect_timeout(Duration::from_secs(10))
        .connect()
        .await?;

    let mut slots = HashMap::new();
    slots.insert(
        "engine-slots".to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: Some(true),
            ..Default::default()
        },
    );
    let mut accounts = HashMap::new();
    if !cfg.account_owners.is_empty() {
        accounts.insert(
            "amm-pools".to_string(),
            SubscribeRequestFilterAccounts {
                owner: cfg.account_owners.clone(),
                ..Default::default()
            },
        );
    }
    let request = SubscribeRequest {
        slots,
        accounts,
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    };

    let (mut sink, mut stream) = client.subscribe_with_request(Some(request)).await?;
    tracing::info!("connected to Yellowstone gRPC at {}", cfg.endpoint);

    while let Some(update) = stream.next().await {
        match update?.update_oneof {
            Some(UpdateOneof::Slot(slot)) => {
                if slot.status == SlotStatus::SlotProcessed as i32
                    && slot.slot > latest_slot.load(Ordering::Relaxed)
                {
                    latest_slot.store(slot.slot, Ordering::Relaxed);
                    // Never block the gRPC stream on a slow consumer.
                    let _ = slot_tx.try_send(SlotEvent {
                        slot: slot.slot,
                        received_at: Instant::now(),
                    });
                }
            }
            Some(UpdateOneof::Account(_)) => {
                account_updates.fetch_add(1, Ordering::Relaxed);
            }
            Some(UpdateOneof::Ping(_)) => {
                sink.send(SubscribeRequest {
                    ping: Some(SubscribeRequestPing { id: 1 }),
                    ..Default::default()
                })
                .await?;
            }
            _ => {}
        }
    }
    anyhow::bail!("Yellowstone stream closed")
}
