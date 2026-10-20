use crate::protocol::protocol::BlockInventory;
use crate::state::{NodeState, TransactionStatus, TrackedTx};
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::info;
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

/// Start the background status worker.
///
/// Runs three concurrent tasks:
/// 1. Heartbeat logging (chain head, peer count, tracking stats)
/// 2. Gap filler (broadcasts sync requests when chain has gaps)
/// 3. Confirmation loop (promotes tracked txs to SOLID from the local chain)
///
/// `solid_depth` comes from `--solid-depth` (default
/// [`crate::config::DEFAULT_SOLID_DEPTH`]).
pub async fn start_status_worker(state: Arc<NodeState>, solid_depth: u64) {
    info!("Starting Status Worker (local chain only, solid depth: {})", solid_depth);

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

    // Task 3: Local finality loop.
    //
    // MINED is assigned when the block containing the transaction arrives over
    // P2P (see `tx_processor`). A transaction becomes SOLID once the local
    // chain has built `solid_depth` blocks on top of it, so no external RPC is
    // needed: the node derives the whole lifecycle from the chain it synced.
    //
    // Transactions seen only in the mempool never get a block number. They
    // stay PENDING until `prune_tracked_txs` (see `main`) drops them after
    // one hour; if their block is observed later, `tx_processor` upgrades them
    // to MINED and this loop eventually promotes them to SOLID.
    let state_c = state.clone();
    tokio::spawn(async move {
        loop {
            let (head, _) = state_c.last_block();
            let solid_num = solid_threshold(head, solid_depth);

            let ready: Vec<(String, TrackedTx)> = state_c
                .tracked_txs
                .iter()
                .filter(|entry| is_solid(entry.value().block_num, solid_num))
                .map(|entry| (entry.key().clone(), entry.value().clone()))
                .collect();

            for (txid, mut tracked) in ready {
                tracked.status = TransactionStatus::Solid;
                state_c.tracked_txs.remove(&txid);
                broadcast_status_update(&txid, &tracked, &state_c);
            }

            sleep(Duration::from_secs(5)).await;
        }
    });
}

/// Highest block number whose transactions are considered final at `head`.
///
/// Saturates at zero while the chain is shorter than `depth`, so the node can
/// start syncing at genesis without underflowing.
#[must_use]
fn solid_threshold(head: u64, depth: u64) -> u64 {
    head.saturating_sub(depth)
}

/// Whether a transaction is final at the given finality threshold.
///
/// Transactions without a block number (still in the mempool) are never final.
#[must_use]
fn is_solid(block_num: Option<u64>, solid_num: u64) -> bool {
    block_num.is_some_and(|block_num| block_num <= solid_num)
}

fn broadcast_status_update(txid: &str, tracked: &crate::state::TrackedTx, state: &NodeState) {
    let event_json = serde_json::json!({ "txid": txid, "type": tracked.tx_type, "status": tracked.status.as_str(), "update_type": "STATUS_CHANGE", "details": tracked.details });
    let event = crate::api::api_proto::TransactionEvent { transaction_json: event_json.to_string() };
    let addrs_ref: Vec<&str> = tracked.notify_addresses.iter().map(|s| s.as_str()).collect();
    state.notify_subscribers(&addrs_ref, event);
}

#[cfg(test)]
mod tests {
    use super::{is_solid, solid_threshold};

    #[test]
    fn solid_threshold_uses_configured_depth() {
        assert_eq!(solid_threshold(1_000, 20), 980);
        assert_eq!(solid_threshold(1_000, 19), 981);
        assert_eq!(solid_threshold(1_000, 1), 999);
    }

    #[test]
    fn solid_threshold_saturates_near_genesis() {
        assert_eq!(solid_threshold(0, 20), 0);
        assert_eq!(solid_threshold(5, 20), 0);
        assert_eq!(solid_threshold(20, 20), 0);
        assert_eq!(solid_threshold(21, 20), 1);
    }

    #[test]
    fn is_solid_at_or_below_threshold() {
        assert!(is_solid(Some(980), 980));
        assert!(is_solid(Some(1), 980));
        assert!(is_solid(Some(0), 0));
    }

    #[test]
    fn is_solid_rejects_above_threshold() {
        assert!(!is_solid(Some(981), 980));
    }

    #[test]
    fn is_solid_rejects_mempool_transactions() {
        assert!(!is_solid(None, 980));
        assert!(!is_solid(None, u64::MAX));
    }
}
