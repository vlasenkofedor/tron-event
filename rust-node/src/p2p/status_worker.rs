use crate::protocol::protocol::wallet_client::WalletClient;
use crate::protocol::protocol::{BytesMessage, EmptyMessage, BlockInventory};
use crate::state::{NodeState, TransactionStatus, TrackedTx};
use std::sync::Arc;
use tokio::time::{sleep, Duration, timeout};
use tracing::{error, info};
use futures::stream::{self, StreamExt};
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

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
            let gossip_head = state_h.head_gossip_num.load(Ordering::SeqCst);
            let tracking = state_h.tracked_txs.len();
            let gaps = state_h.pending_gaps.lock().unwrap().len();
            let peers = state_h.active_peers.len();
            
            if gaps > 0 {
                info!("[HEARTBEAT] SeqHead: {}, GossipHead: {}, GAPS: {}, Peers: {}, Tracking: {}", seq_head, gossip_head, gaps, peers, tracking);
            } else {
                info!("[HEARTBEAT] Sync Status: OK, Head: {}, Peers: {}, Tracking: {}", seq_head, peers, tracking);
            }
        }
    });

    // Task 2: Advanced Gap Sync Worker (Broadcasts with Logarithmic locators)
    let state_g = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(3000));
        loop {
            interval.tick().await;
            let gaps_count = { state_g.pending_gaps.lock().unwrap().len() };

            if gaps_count > 0 {
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
                let last = state_g.last_sync_request_time.load(Ordering::SeqCst);

                if now.saturating_sub(last) > 3000 {
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
                }
            }
        }
    });

    let mut client = client;
    loop {
        // gRPC fallback if sync is idle
        let get_now = timeout(Duration::from_secs(3), client.get_now_block(EmptyMessage {}));
        if let Ok(Ok(resp)) = get_now.await {
            let block = resp.into_inner();
            if let Some(header) = block.block_header.as_ref().and_then(|h| h.raw_data.as_ref()) {
                let block_id = crate::p2p::utils::compute_block_id(header.number, header);
                state.update_block(header.number as u64, &block_id, &header.parent_hash);
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
