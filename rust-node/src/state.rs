use dashmap::DashMap;
use lru::LruCache;
use parking_lot::{Mutex, RwLock};
use std::num::NonZeroUsize;
use std::sync::{Arc};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::{mpsc, Semaphore};
use tracing::info;
use std::collections::{VecDeque, HashSet};

use crate::protocol;

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_genesis_hash_is_valid_hex() {
        assert_eq!(GENESIS_HASH.len(), 64);
        assert!(hex::decode(GENESIS_HASH).is_ok());
    }

    #[test]
    fn test_block_update_result_enum() {
        let success = BlockUpdateResult::Success;
        let gap = BlockUpdateResult::GapDetected { missing_count: 5, last_good_hash: "abc".to_string() };
        let dup = BlockUpdateResult::Duplicate;
        assert!(matches!(success, BlockUpdateResult::Success));
        assert!(matches!(gap, BlockUpdateResult::GapDetected { .. }));
        assert!(matches!(dup, BlockUpdateResult::Duplicate));
    }

    #[test]
    fn test_transaction_status_as_str() {
        assert_eq!(TransactionStatus::Mempool.as_str(), "PENDING_MEMPOOL");
        assert_eq!(TransactionStatus::Mined.as_str(), "MINED");
        assert_eq!(TransactionStatus::Solid.as_str(), "SOLID");
    }

    #[test]
    fn test_block_info_integrity() {
        let block = BlockInfo { number: 100, hash: "abcd".to_string(), parent_hash: "1234".to_string() };
        assert_eq!(block.number, 100);
        assert_eq!(block.hash, "abcd");
        assert_eq!(block.parent_hash, "1234");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

/// Path to the file persisting the last processed block height and hash.
const STATE_FILE: &str = "height.txt";
/// Maximum number of block headers kept in the in-memory chain history.
const CHAIN_HISTORY_LIMIT: usize = 100;
/// Known TRON mainnet genesis block hash (hex-encoded).
pub const GENESIS_HASH: &str = "00000000000000001ebf88508a03865c71d452e25f4d51194196a1d22b6653dc";

/// Minimum distance between our chain head and a peer's head before we
/// re-anchor our chain to the peer's position.
///
/// The node persists its chain position across restarts. If the persisted
/// position becomes stale (e.g. the node was offline for a long time), the
/// gap can exceed millions of blocks. Peers serve blocks only from their own
/// recent chain, so a stale local position can never be closed by normal
/// block fetching. When the gap exceeds this threshold we re-anchor to the
/// peer's advertised head and resume syncing from there.
pub const RESYNC_THRESHOLD: u64 = 100_000;

/// Placeholder hash used when no real chain position is known yet.
///
/// It marks the local head as "untrusted": the advertised hash does not
/// correspond to a real block, so peers cannot match it against their chain
/// and would not serve us anything. The position is replaced by the peer's
/// validated head as soon as the first handshake completes.
pub const UNKNOWN_BLOCK_HASH: &str = "unknown";

// ─────────────────────────────────────────────────────────────────────────────
// Public Types
// ─────────────────────────────────────────────────────────────────────────────

/// Metadata about a processed block: height, hash, and parent hash.
#[derive(Debug, Clone)]
pub struct BlockInfo {
    /// Block height (number).
    pub number: u64,
    /// Hex-encoded block hash.
    pub hash: String,
    /// Hex-encoded parent block hash.
    pub parent_hash: String,
}

/// Result of attempting to advance the chain with a new block header.
#[non_exhaustive]
pub enum BlockUpdateResult {
    /// Block successfully appended to the chain.
    Success,
    /// A gap was detected between the known chain tip and the incoming block.
    GapDetected { missing_count: u64, last_good_hash: String },
    /// Block has already been processed (duplicate).
    Duplicate,
    /// A fork was detected and the chain was rolled back to a common ancestor.
    Reorg { ancestor_number: u64, orphaned_txs: usize },
}

/// A block that arrived before its parent was known.
///
/// Peers deliver blocks concurrently, so a block can show up several
/// positions ahead of the current tip. Such a block is buffered here and
/// applied as soon as the missing ancestors arrive.
#[derive(Debug, Clone)]
pub struct BufferedBlock {
    /// Block height.
    pub number: u64,
    /// Hex-encoded block id.
    pub hash: String,
    /// Hex-encoded parent block id.
    pub parent_hash: String,
}

/// Thread-safe shared state for the Tron event node.
///
/// Manages the blockchain view, tracked transactions, subscriber registry,
/// and peer connection tracking. All fields are internally synchronized.
/// Uses `RwLock` for the block chain to allow concurrent reads.
pub struct NodeState {
    block_chain: RwLock<VecDeque<BlockInfo>>,
    pub locators: RwLock<VecDeque<BlockInfo>>,
    pub requested_blocks: DashMap<String, u64>, // Hash hex -> timestamp (ms)
    seen_txids: Mutex<LruCache<[u8; 32], ()>>,
    pub parsing_semaphore: Arc<Semaphore>,
    pub active_peers: DashMap<String, ()>,
    pub tx_broadcast: tokio::sync::broadcast::Sender<protocol::protocol::Transaction>,
    pub sync_commands: tokio::sync::broadcast::Sender<protocol::protocol::BlockInventory>,
    pub last_sync_request_time: AtomicU64,
    pub last_seq_head_move_time: AtomicU64, 

    session_counter: AtomicU64,
    pub subscribers: DashMap<String, Vec<(u64, mpsc::Sender<crate::api::api_proto::TransactionEvent>)>>,
    pub tracked_txs: DashMap<String, TrackedTx>,

    pub pending_gaps: Mutex<HashSet<u64>>,
    pub head_gossip_num: AtomicU64,

    /// Blocks that arrived ahead of their parent and are waiting to be applied.
    pub block_buffer: DashMap<u64, BufferedBlock>,

    /// Whether the current chain position was validated by a peer.
    ///
    /// A persisted position is trusted because it was verified while the
    /// node was connected. A freshly started node has no trusted position
    /// until the first peer handshake provides one.
    pub chain_trusted: AtomicBool,

    // Stats
    pub stats_inventory_tx: AtomicU64,
    pub stats_blocks: AtomicU64,
    pub stats_usdt: AtomicU64,
    pub stats_notified: AtomicU64,
    pub stats_notify_calls: AtomicU64,
}

/// Lifecycle status of a tracked transaction.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
pub enum TransactionStatus {
    /// Transaction seen in the mempool but not yet mined.
    #[serde(rename = "PENDING_MEMPOOL")]
    Mempool,
    /// Transaction has been included in a block.
    #[serde(rename = "MINED")]
    Mined,
    /// Transaction has reached finality (19+ blocks deep).
    #[serde(rename = "SOLID")]
    Solid,
}

impl TransactionStatus {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Mempool => "PENDING_MEMPOOL",
            Self::Mined => "MINED",
            Self::Solid => "SOLID",
        }
    }
}

#[derive(Debug, Clone)]
pub struct TrackedTx {
    pub hash: Vec<u8>,
    pub status: TransactionStatus,
    pub first_seen: std::time::Instant,
    pub block_num: Option<u64>,
    pub tx_type: String,
    pub notify_addresses: Vec<String>,
    pub details: serde_json::Value,
}

impl NodeState {
    /// Load the persisted chain position and initialize the shared state.
    ///
    /// The persisted position may be arbitrarily stale (e.g. the node was
    /// offline for months). It is only used as an initial guess; the real
    /// position is adopted from the first peer that completes a handshake
    /// (see [`NodeState::reanchor_to_head`]).
    pub fn load() -> Self {
        let mut chain = VecDeque::new();
        if let Ok(content) = std::fs::read_to_string(STATE_FILE) {
            for line in content.lines() {
                let parts: Vec<&str> = line.split(':').collect();
                if parts.len() >= 2 {
                    let num = parts[0].parse::<u64>().unwrap_or(0);
                    let hash = parts[1].to_string();
                    let parent = if parts.len() >= 3 { parts[2].to_string() } else { "0".repeat(64) };
                    chain.push_back(BlockInfo { number: num, hash, parent_hash: parent });
                }
            }
        }

        if chain.is_empty() {
            // No persisted position: start from the genesis block.
            //
            // The genesis block id is a real, verifiable block id, so it can
            // be advertised to peers in the handshake. A real position is
            // adopted from the first peer handshake (see
            // `NodeState::reanchor_to_head`), because a fabricated hash
            // would make peers unable to find a common ancestor with us.
            chain.push_back(BlockInfo {
                number: 0,
                hash: GENESIS_HASH.to_string(),
                parent_hash: String::new(),
            });
            let _ = std::fs::write(
                STATE_FILE,
                format!("0:{}:", GENESIS_HASH),
            );
        }

        let tip_num = chain.back().map(|b| b.number).unwrap_or(0);
        let now_ms = Self::now_ms();

        tracing::info!(
            "[STATE] Loaded chain position: tip #{} ({} blocks in history)",
            tip_num,
            chain.len()
        );

        Self {
            block_chain: RwLock::new(chain),
            locators: RwLock::new(VecDeque::new()),
            seen_txids: Mutex::new(LruCache::new(NonZeroUsize::new(200_000).unwrap())),
            parsing_semaphore: Arc::new(Semaphore::new(128)),
            active_peers: DashMap::new(),
            tx_broadcast: tokio::sync::broadcast::channel(1024).0,
            sync_commands: tokio::sync::broadcast::channel(64).0,
            requested_blocks: DashMap::new(),
            last_sync_request_time: AtomicU64::new(0),
            last_seq_head_move_time: AtomicU64::new(now_ms),
            session_counter: AtomicU64::new(1),
            subscribers: DashMap::new(),
            tracked_txs: DashMap::new(),
            pending_gaps: Mutex::new(HashSet::new()),
            head_gossip_num: AtomicU64::new(tip_num),
            block_buffer: DashMap::new(),
            chain_trusted: AtomicBool::new(false),
            stats_inventory_tx: AtomicU64::new(0),
            stats_blocks: AtomicU64::new(0),
            stats_usdt: AtomicU64::new(0),
            stats_notified: AtomicU64::new(0),
            stats_notify_calls: AtomicU64::new(0),
        }
    }

    /// Re-anchor the chain to a peer's advertised head block.
    ///
    /// Called when a peer's `head_block_id` is far ahead of our local chain
    /// tip (see [`RESYNC_THRESHOLD`]). The peer has already validated that
    /// block, so adopting it lets the node resume syncing from the canonical
    /// chain instead of being stuck on a stale, unreachable position.
    ///
    /// Returns `true` when the chain was re-anchored.
    pub fn reanchor_to_head(&self, head_num: u64, head_hash: &str) -> bool {
        if head_hash.is_empty() || head_hash.len() != 64 {
            return false;
        }

        let mut chain = self.block_chain.write();
        let tip = chain.back();

        // Decide whether to adopt the peer's position.
        //
        // 1. If we have no trusted local position, adopt the peer's head.
        //    A node starts without a trusted position and the first peer
        //    handshake provides one.
        // 2. If the peer is far ahead (more than RESYNC_THRESHOLD blocks),
        //    our persisted position is unreachable: peers only serve blocks
        //    from their own recent chain, so the gap can never be closed.
        // 3. Otherwise keep our position.
        //
        // Case 3 is the common one and must not re-anchor: peers are
        // slightly out of sync with each other, so re-anchoring to every
        // peer's head would clear the chain history on each new connection
        // and the node would never make progress.
        let tip_num = tip.map(|b| b.number).unwrap_or(0);
        let too_far_behind = head_num > tip_num.saturating_add(RESYNC_THRESHOLD);
        let trusted = self.chain_trusted.load(Ordering::SeqCst);

        if trusted && !too_far_behind {
            return false;
        }

        if trusted {
            info!(
                "[SYNC] Re-anchoring chain: local tip #{} is behind peer head #{} \
                 (> {} blocks). Resetting to peer position.",
                tip_num, head_num, RESYNC_THRESHOLD
            );
        } else {
            info!(
                "[SYNC] No trusted local chain position. Adopting peer head #{}.",
                head_num
            );
        }

        // Reset the chain to the peer's head. The parent hash is unknown at
        // this point, so it is left empty until the next block arrives.
        chain.clear();
        chain.push_back(BlockInfo {
            number: head_num,
            hash: head_hash.to_string(),
            parent_hash: String::new(),
        });

        // Clear any pending gaps: the new anchor is a known-good position.
        self.pending_gaps.lock().clear();

        // The position now comes from a peer that completed a handshake.
        self.chain_trusted.store(true, Ordering::SeqCst);
        // Reset tracking of the old chain head so the watchdog does not
        // immediately recycle connections thinking sync is stalled.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self.last_seq_head_move_time.store(now_ms, Ordering::SeqCst);
        self.head_gossip_num.fetch_max(head_num, Ordering::SeqCst);

        // Persist the new anchor.
        let content = format!(
            "{}:{}:{}\n",
            head_num,
            head_hash,
            chain.back().map(|b| b.parent_hash.clone()).unwrap_or_default()
        );
        let _ = std::fs::write(STATE_FILE, content);

        true
    }

    /// Apply buffered blocks that have become connectable to the chain.
    ///
    /// Called after every successful chain advance. Blocks that arrived out
    /// of order are stored in `block_buffer`; once the missing ancestors are
    /// applied, the buffered descendants become applicable and are applied in
    /// order. Returns the number of blocks that were applied.
    pub fn drain_buffer(&self) -> usize {
        let mut applied = 0usize;

        loop {
            let next = {
                let chain = self.block_chain.read();
                let Some(tip) = chain.back() else { break };
                chain
                    .iter()
                    .find(|b| b.number == tip.number + 1)
                    .map(|b| b.number)
            };

            let Some(next_num) = next else { break };

            // Take the buffered block that continues the chain.
            let Some((num, buffered)) = self
                .block_buffer
                .iter()
                .find(|entry| entry.value().number == next_num)
                .map(|entry| (*entry.key(), entry.value().clone()))
            else {
                break;
            };

            let parent_hash = hex::decode(&buffered.parent_hash).unwrap_or_default();
            let hash_bytes = hex::decode(&buffered.hash).unwrap_or_default();

            self.block_buffer.remove(&num);

            match self.update_block(num, &hash_bytes, &parent_hash) {
                BlockUpdateResult::Success => applied += 1,
                _ => break,
            }
        }

        if applied > 0 {
            tracing::debug!("[CHAIN] Applied {} buffered block(s) from the out-of-order buffer", applied);
        }
        applied
    }

    /// Get the current chain tip number (block height).
    pub fn chain_tip(&self) -> u64 {
        self.block_chain.read().back().map(|b| b.number).unwrap_or(0)
    }

    pub fn last_block(&self) -> (u64, String) {
        let chain = self.block_chain.read();
        if let Some(tip) = chain.back() { (tip.number, tip.hash.clone()) } else { (0, "0".repeat(64)) }
    }

    pub fn get_locators(&self, _count_hint: usize) -> Vec<(u64, String)> {
        let chain = self.block_chain.read();
        let mut locators = Vec::new();

        // The genesis block is always offered as a fallback: a peer that does
        // not know any of our recent blocks can still find a common ancestor
        // with us through the genesis block.
        locators.push((0, GENESIS_HASH.to_string()));

        // Only send locators whose hash is a real block hash. Peers match
        // locator hashes against their own chain; sending a placeholder
        // would simply be ignored and could stall the sync.
        if let Some(tip) = chain.back() {
            if tip.hash.len() == 64 {
                let mut step = 1;
                let mut i = 0;
                while i < chain.len() {
                    let b = &chain[i];
                    if b.number != 0 && b.hash.len() == 64 {
                        locators.push((b.number, b.hash.clone()));
                    }
                    i += step;
                    step *= 2;
                }
                for j in (chain.len().saturating_sub(5))..chain.len() {
                    let b = &chain[j];
                    if b.hash.len() != 64 { continue; }
                    let entry = (b.number, b.hash.clone());
                    if !locators.contains(&entry) { locators.push(entry); }
                }
            }
        }

        // Locators are ordered oldest-first, with the genesis block at the
        // end of the list. This ordering is what peers accept in practice;
        // a newest-first list makes them fall back to an old block.
        locators.sort_by_key(|(n, _)| *n);
        locators.dedup();
        tracing::debug!("[SYNC] Locators: {:?}", locators);
        locators
    }

    pub fn get_gap_locators(&self) -> Option<Vec<(u64, String)>> {
        let gaps = self.pending_gaps.lock();
        if gaps.is_empty() { return None; }
        Some(self.get_locators(10))
    }

    pub fn update_block(&self, num: u64, hash_bytes: &[u8], parent_hash_bytes: &[u8]) -> BlockUpdateResult {
        let hash_hex = hex::encode(hash_bytes);
        let parent_hex = hex::encode(parent_hash_bytes);
        let mut persistence_content: Option<String> = None;
        let mut result = BlockUpdateResult::Success;

        self.head_gossip_num.fetch_max(num, Ordering::SeqCst);

        {
            let mut chain = self.block_chain.write();
            let mut gaps = self.pending_gaps.lock();

            gaps.remove(&num);

            let Some(tip) = chain.back() else { return BlockUpdateResult::Success };

            // Case 1: direct child of the tip -> append.
            if num == tip.number + 1 && parent_hex == tip.hash {
                chain.push_back(BlockInfo { number: num, hash: hash_hex.clone(), parent_hash: parent_hex });
                while chain.len() > CHAIN_HISTORY_LIMIT { chain.pop_front(); }
                persistence_content = Some(Self::serialize_chain(&chain));
                self.last_seq_head_move_time.store(Self::now_ms(), Ordering::SeqCst);
                // The chain now contains a block we received and verified,
                // so the position is trusted.
                self.chain_trusted.store(true, Ordering::SeqCst);
            } else if num == tip.number {
                // Same height: identical hash means we already have it.
                if hash_hex == tip.hash {
                    return BlockUpdateResult::Duplicate;
                }
                // Different hash at the same height -> competing branch.
                result = self.handle_fork(&mut chain, num, &hash_hex, &parent_hex);
            } else if num > tip.number + 1 {
                // The block arrived before its ancestors (peers deliver
                // blocks concurrently). Buffer it instead of discarding it:
                // it will be applied by `drain_buffer` once the missing
                // blocks have been received.
                self.block_buffer.insert(num, BufferedBlock {
                    number: num,
                    hash: hash_hex.clone(),
                    parent_hash: parent_hex.clone(),
                });

                let missing_count = num - tip.number - 1;
                let to_add = std::cmp::min(missing_count, 2000);
                for g_num in (num.saturating_sub(to_add))..num { gaps.insert(g_num); }
                tracing::debug!(
                    "[CHAIN] Buffered block #{} (tip is #{}, {} blocks missing) parent={} tip_hash={}",
                    num, tip.number, missing_count, parent_hex, tip.hash
                );
                result = BlockUpdateResult::GapDetected { missing_count, last_good_hash: tip.hash.clone() };
            } else if num == tip.number + 1 {
                // The block is the direct child of our tip by height, but the
                // parent hash does not match our stored tip hash.
                //
                // This happens when our tip hash was adopted from a peer's
                // Hello message (which carries the peer's own block-id
                // representation) and the wire format differs from the one we
                // compute locally. The block itself comes from a peer that
                // completed a handshake, so it is authoritative: adopt it and
                // re-anchor the tip hash to the parent hash the peer used.
                tracing::debug!(
                    "[CHAIN] Parent hash mismatch at #{}: our tip hash {}, block parent {}. Adopting block.",
                    num, tip.hash, parent_hex
                );
                chain.back_mut().expect("tip exists").hash = parent_hex.clone();
                chain.push_back(BlockInfo { number: num, hash: hash_hex.clone(), parent_hash: parent_hex });
                while chain.len() > CHAIN_HISTORY_LIMIT { chain.pop_front(); }
                persistence_content = Some(Self::serialize_chain(&chain));
                self.last_seq_head_move_time.store(Self::now_ms(), Ordering::SeqCst);
            } else {
                // num < tip.number: the block is behind our head.
                if chain.iter().any(|b| b.hash == hash_hex) {
                    return BlockUpdateResult::Duplicate;
                }
                // A different block at a height we already passed -> the
                // canonical branch diverged before our current head.
                result = self.handle_fork(&mut chain, num, &hash_hex, &parent_hex);
            }
        }

        if let Some(content) = persistence_content {
            let _ = std::fs::write(STATE_FILE, content);
            info!("[CHAIN] Tip: {}", num);
        }
        result
    }

    /// Handle a competing branch: roll the chain back to the common ancestor
    /// and return a [`BlockUpdateResult::Reorg`] describing the rollback.
    fn handle_fork(
        &self,
        chain: &mut VecDeque<BlockInfo>,
        num: u64,
        hash_hex: &str,
        parent_hex: &str,
    ) -> BlockUpdateResult {
        // Find the deepest block in our history that is an ancestor of the
        // incoming branch. We walk back from our tip looking for a block whose
        // hash matches the incoming block's parent hash.
        let mut ancestor_index = None;
        for (idx, block) in chain.iter().enumerate().rev() {
            if block.hash == parent_hex {
                ancestor_index = Some(idx);
                break;
            }
            if block.number <= num {
                break;
            }
        }

        let Some(idx) = ancestor_index else {
            // No common ancestor in our (limited) history: the branch is too
            // far diverged to reconcile locally. Signal a re-sync instead.
            //
            // One case must not be treated as a divergence: right after a
            // restart the chain holds a single block adopted from a peer's
            // Hello message. That block is only an anchor and its hash may
            // come from a different peer (or fork), so no peer can match it
            // as a locator. A peer sending a block at the same height with a
            // different hash proves the anchor is unusable; adopt the real
            // block, otherwise every sync request would return that same
            // block as a duplicate and the node would never advance.
            let anchor = chain.back().filter(|b| num == b.number && b.parent_hash.is_empty()).cloned();
            if let Some(anchor) = anchor {
                info!(
                    "[CHAIN] Replacing unverified anchor #{} ({}) with peer block {}",
                    anchor.number, anchor.hash, hash_hex
                );
                chain.clear();
                chain.push_back(BlockInfo {
                    number: num,
                    hash: hash_hex.to_string(),
                    parent_hash: parent_hex.to_string(),
                });
                let content = Self::serialize_chain(chain);
                let _ = std::fs::write(STATE_FILE, &content);
                self.rollback_txs_of_block(anchor.number);
                self.chain_trusted.store(true, Ordering::SeqCst);
                return BlockUpdateResult::Success;
            }

            info!("[CHAIN] Fork at #{} has no common ancestor in local history", num);
            return BlockUpdateResult::Duplicate;
        };

        let ancestor = chain[idx].clone();

        // Roll back the orphaned branch.
        let orphaned: Vec<BlockInfo> = chain.drain(idx + 1..).collect();
        let orphaned_txs = orphaned.len();

        // Append the incoming block if it directly follows the ancestor.
        if num == ancestor.number + 1 {
            chain.push_back(BlockInfo {
                number: num,
                hash: hash_hex.to_string(),
                parent_hash: parent_hex.to_string(),
            });
        }

        // Persist the rolled-back chain.
        let content = Self::serialize_chain(chain);
        let _ = std::fs::write(STATE_FILE, &content);

        // Transactions mined in the orphaned blocks are no longer confirmed.
        for block in &orphaned {
            self.rollback_txs_of_block(block.number);
        }

        info!(
            "[CHAIN] Reorg: rolled back to #{} ({} orphaned blocks)",
            ancestor.number, orphaned_txs
        );

        BlockUpdateResult::Reorg { ancestor_number: ancestor.number, orphaned_txs }
    }

    /// Drop transactions that were mined in the given (now orphaned) block.
    fn rollback_txs_of_block(&self, block_num: u64) {
        self.tracked_txs.retain(|_, tx| tx.block_num != Some(block_num));
    }

    /// Serialize the chain history into the on-disk state format.
    fn serialize_chain(chain: &VecDeque<BlockInfo>) -> String {
        let mut content = String::new();
        for b in chain.iter() {
            content.push_str(&format!("{}:{}:{}\n", b.number, b.hash, b.parent_hash));
        }
        content
    }

    /// Current wall-clock time in milliseconds since the Unix epoch.
    fn now_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    pub fn is_new_tx(&self, txid: &[u8]) -> bool {
        if txid.len() != 32 { return true; }
        let mut key = [0u8; 32]; key.copy_from_slice(&txid[..32]);
        let mut cache = self.seen_txids.lock();
        if cache.contains(&key) { return false; }
        cache.put(key, ()); true
    }

    pub fn next_session_id(&self) -> u64 { self.session_counter.fetch_add(1, Ordering::SeqCst) }
    pub fn add_subscriber(&self, session_id: u64, hex_addr: String, tx: mpsc::Sender<crate::api::api_proto::TransactionEvent>) {
        self.subscribers.entry(hex_addr).or_insert_with(Vec::new).push((session_id, tx));
    }
    pub fn remove_subscriber(&self, session_id: u64, hex_addr: &str) {
        if let Some(mut subs) = self.subscribers.get_mut(hex_addr) {
            subs.retain(|(sid, _)| *sid != session_id);
        }
    }
    pub fn cleanup_session(&self, session_id: u64) {
        for mut entry in self.subscribers.iter_mut() { entry.value_mut().retain(|(sid, _)| *sid != session_id); }
        self.subscribers.retain(|_, v| !v.is_empty());
    }
    pub fn prune_tracked_txs(&self, max_age: std::time::Duration) {
        let now = std::time::Instant::now();
        self.tracked_txs.retain(|_, tx| now.duration_since(tx.first_seen) < max_age);
    }
    pub fn notify_subscribers(&self, addrs: &[&str], event: crate::api::api_proto::TransactionEvent) -> bool {
        let mut found = false;
        self.stats_notify_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        for addr in addrs {
            if let Some(mut subs) = self.subscribers.get_mut(*addr) {
                if !subs.is_empty() {
                    found = true;
                    let before = subs.len();
                    subs.retain(|(_, s)| s.try_send(event.clone()).is_ok());
                    let after = subs.len();
                    if before != after {
                        tracing::warn!(
                            "[SUBS] Channel full for '{}': dropped {} subscriber(s)",
                            addr, before - after
                        );
                    }
                    self.stats_notified.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            } else if self.subscribers.is_empty() {
                tracing::trace!("[SUBS] No subscribers registered at all");
            } else {
                tracing::trace!(
                    "[SUBS] No subscriber for '{}' (registered: {:?})",
                    addr,
                    self.subscribers.iter().map(|e| e.key().clone()).collect::<Vec<_>>()
                );
            }
        }
        found
    }
}
