use crate::protocol::protocol::wallet_client::WalletClient;
use crate::protocol::protocol::{BytesMessage, EmptyMessage, BlockInventory};
use crate::state::{NodeState, TransactionStatus, TrackedTx};
use std::sync::Arc;
use tokio::time::{sleep, Duration, timeout};
use tracing::{error, info};
use futures::stream::{self, StreamExt};
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

/// Start the background status worker.
///
/// Runs three concurrent tasks:
/// 1. Heartbeat logging (chain head, peer count, tracking stats)
/// 2. Gap filler (broadcasts sync requests when chain has gaps)
/// 3. Confirmation loop (checks tracked txs for mined/solid status via gRPC)
pub async fn start_status_worker(state: Arc<NodeState>, grpc_url: String) {
    info!("Starting Status Worker (gRPC: {})", grpc_url);

    let client = match WalletClient::connect(format!("http://{}", grpc_url)).await {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to connect to Tron gRPC ({}): {}", grpc_url, e);
            return;
        }
    };

    // Task 1: Heartbeat logging (Now with Peer Count)
    let state_h = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await;
            let (seq_head, _) = state_h.last_block();
            let _gossip_head = state_h.head_gossip_num.load(Ordering::SeqCst);
            let tracking = state_h.tracked_txs.len();
            let _gaps = state_h.pending_gaps.lock().len();
            let peers = state_h.active_peers.len();
            let notified = state_h.stats_notified.load(Ordering::Relaxed);
            let notify_calls = state_h.stats_notify_calls.load(Ordering::Relaxed);
            let subscribers = state_h.subscribers.len();

            info!("[HEARTBEAT] Sync Status: OK, Head: {}, Peers: {}, Tracking: {}, Subs: {}, Notified: {}/{}",
                seq_head, peers, tracking, subscribers, notified, notify_calls);
        }
    });

    // Task 2: Advanced Gap Sync Worker (Broadcasts with Logarithmic locators)
    //
    // The worker keeps the node in sync with the canonical chain:
    //   - When gaps exist, it broadcasts a sync request with our locators so
    //     peers can send us the missing blocks.
    //   - When the chain is up to date, it still polls periodically so newly
    //     produced blocks are discovered. Without this the node would only
    //     advance when a peer happened to announce a block.
    let state_g = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(3000));
        let mut last_poll = std::time::Instant::now() - Duration::from_secs(60);
        loop {
            interval.tick().await;
            let gaps_count = { state_g.pending_gaps.lock().len() };

            let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0);
            let last = state_g.last_sync_request_time.load(Ordering::SeqCst);

            // Minimum interval between two sync broadcasts (3s).
            if now.saturating_sub(last) < 3000 {
                continue;
            }

            if gaps_count > 0 {
                if let Some(locators) = state_g.get_gap_locators() {
                    let ids = locators.into_iter().map(|(num, hash_hex)| {
                         crate::protocol::protocol::block_inventory::BlockId {
                             hash: hex::decode(hash_hex).unwrap_or_default(),
                             number: num as i64,
                         }
                    }).collect();

                    let sync_req = BlockInventory { ids, r#type: 0 };
                    state_g.last_sync_request_time.store(now, Ordering::SeqCst);
                    info!("[GAP_FILLER] Broadcasting Sync with Logarithmic Locators (Gaps: {})", gaps_count);
                    let _ = state_g.sync_commands.send(sync_req);
                }
            } else if last_poll.elapsed() >= Duration::from_secs(3) {
                // Chain is up to date: ask peers for the next window of
                // blocks every 3 seconds.
                //
                // A peer answers a SyncBlockChain request with a limited
                // window of block ids (see `remain_num`). Once that window
                // has been fetched, the only way to get the following blocks
                // is to ask again with our updated tip.
                let locators = state_g.get_locators(10);
                let ids = locators.into_iter().map(|(num, hash_hex)| {
                     crate::protocol::protocol::block_inventory::BlockId {
                         hash: hex::decode(hash_hex).unwrap_or_default(),
                         number: num as i64,
                     }
                }).collect();

                let sync_req = BlockInventory { ids, r#type: 0 };
                state_g.last_sync_request_time.store(now, Ordering::SeqCst);
                last_poll = std::time::Instant::now();
                let _ = state_g.sync_commands.send(sync_req);
            }
        }
    });

    let mut client = client;
    loop {
        // gRPC fallback if sync is idle.
        //
        // The upstream node may be on a different fork or a different chain
        // position than our P2P peers, so its block must NOT be applied to
        // our local chain: doing so would insert thousands of phantom gap
        // entries and prevent the P2P sync from ever closing them.
        // We only use it to learn the current network head.
        let get_now = timeout(Duration::from_secs(3), client.get_now_block(EmptyMessage {}));
        if let Ok(Ok(resp)) = get_now.await {
            let block = resp.into_inner();
            if let Some(header) = block.block_header.as_ref().and_then(|h| h.raw_data.as_ref()) {
                let number = header.number as u64;
                state.head_gossip_num.fetch_max(number, Ordering::Relaxed);
            }
        }

        let (last_num, _) = state.last_block();
        let last_solid_num = last_num.saturating_sub(20);
        let tracked_list: Vec<(String, TrackedTx)> = state.tracked_txs.iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();

        if !tracked_list.is_empty() {
            let client_clone = client.clone();
            let results = stream::iter(tracked_list)
                .map(|(txid, mut tracked)| {
                    let mut client_inner = client_clone.clone();
                    let last_solid_inner = last_solid_num;
                    async move {
                        let mut update: Option<TrackedTx> = None;
                        let mut remove = false;
                        if let Some(bn) = tracked.block_num {
                            if bn <= last_solid_inner {
                                tracked.status = TransactionStatus::Solid;
                                update = Some(tracked.clone());
                                remove = true;
                            }
                        } else {
                            let req = BytesMessage { value: tracked.hash.clone() };
                            if let Ok(Ok(resp)) = timeout(Duration::from_secs(3), client_inner.get_transaction_info_by_id(req)).await {
                                let info = resp.into_inner();
                                if info.block_number > 0 {
                                    tracked.status = TransactionStatus::Mined;
                                    tracked.block_num = Some(info.block_number as u64);
                                    update = Some(tracked.clone());
                                }
                            } else if tracked.first_seen.elapsed() > Duration::from_secs(600) {
                                remove = true;
                            }
                        }
                        (txid, update, remove)
                    }
                })
                .buffer_unordered(20)
                .collect::<Vec<(String, Option<TrackedTx>, bool)>>()
                .await;

            for (txid, update, remove) in results {
                if let Some(t) = update { state.tracked_txs.insert(txid.clone(), t.clone()); broadcast_status_update(&txid, &t, &state); }
                if remove { state.tracked_txs.remove(&txid); }
            }
        }
        sleep(Duration::from_secs(5)).await;
    }
}

fn broadcast_status_update(txid: &str, tracked: &crate::state::TrackedTx, state: &NodeState) {
    let event_json = serde_json::json!({ "txid": txid, "type": tracked.tx_type, "status": tracked.status.as_str(), "update_type": "STATUS_CHANGE", "details": tracked.details });
    let event = crate::api::api_proto::TransactionEvent { transaction_json: event_json.to_string() };
    let addrs_ref: Vec<&str> = tracked.notify_addresses.iter().map(|s| s.as_str()).collect();
    state.notify_subscribers(&addrs_ref, event);
}
