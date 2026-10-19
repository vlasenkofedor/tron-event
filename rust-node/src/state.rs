use dashmap::DashMap;
use lru::LruCache;
use parking_lot::{Mutex, RwLock};
use std::num::NonZeroUsize;
use std::sync::{Arc};
use std::sync::atomic::{AtomicU64, Ordering};
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

    // Stats
    pub stats_inventory_tx: AtomicU64,
    pub stats_blocks: AtomicU64,
    pub stats_usdt: AtomicU64,
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
            let height = crate::config::DEFAULT_BLOCK_NUM;
            let hash = crate::config::DEFAULT_BLOCK_HASH.to_string();
            chain.push_back(BlockInfo { number: height, hash: hash.clone(), parent_hash: "0".repeat(64) });
            let _ = std::fs::write(STATE_FILE, format!("{}:{}:{}", height, hash, "0".repeat(64)));
        }

        let tip_num = chain.back().map(|b| b.number).unwrap_or(0);
        let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;

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
            stats_inventory_tx: AtomicU64::new(0),
            stats_blocks: AtomicU64::new(0),
            stats_usdt: AtomicU64::new(0),
        }
    }

    pub fn last_block(&self) -> (u64, String) {
        let chain = self.block_chain.read();
        if let Some(tip) = chain.back() { (tip.number, tip.hash.clone()) } else { (0, "0".repeat(64)) }
    }

    pub fn get_locators(&self, _count_hint: usize) -> Vec<(u64, String)> {
        let chain = self.block_chain.read();
        let mut locators = Vec::new();
        locators.push((0, GENESIS_HASH.to_string()));

        if let Some(_tip) = chain.back() {
            let mut step = 1;
            let mut i = 0;
            while i < chain.len() {
                let b = &chain[i];
                if b.number != 0 { locators.push((b.number, b.hash.clone())); }
                i += step;
                step *= 2; 
            }
            for j in (chain.len().saturating_sub(5))..chain.len() {
                let b = &chain[j];
                let entry = (b.number, b.hash.clone());
                if !locators.contains(&entry) { locators.push(entry); }
            }
        }
        locators.sort_by_key(|(n, _)| *n);
        locators.dedup();
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

            if let Some(tip) = chain.back() {
                if num == tip.number + 1 && parent_hex == tip.hash {
                    chain.push_back(BlockInfo { number: num, hash: hash_hex.clone(), parent_hash: parent_hex });
                    while chain.len() > CHAIN_HISTORY_LIMIT { chain.pop_front(); }
                    let mut content = String::new();
                    for b in chain.iter() { content.push_str(&format!("{}:{}:{}\n", b.number, b.hash, b.parent_hash)); }
                    persistence_content = Some(content);
                    let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
                    self.last_seq_head_move_time.store(now_ms, Ordering::SeqCst);
                } else if num > tip.number + 1 {
                    let missing_count = num - tip.number - 1;
                    let to_add = std::cmp::min(missing_count, 2000);
                    for g_num in (num.saturating_sub(to_add))..num { gaps.insert(g_num); }
                    result = BlockUpdateResult::GapDetected { missing_count, last_good_hash: tip.hash.clone() };
                } else if num == tip.number {
                    if hash_hex == tip.hash { return BlockUpdateResult::Duplicate; }
                } else {
                    if num == tip.number + 1 && parent_hex == tip.hash {
                         chain.push_back(BlockInfo { number: num, hash: hash_hex.clone(), parent_hash: parent_hex });
                         while chain.len() > CHAIN_HISTORY_LIMIT { chain.pop_front(); }
                         let mut content = String::new();
                         for b in chain.iter() { content.push_str(&format!("{}:{}:{}\n", b.number, b.hash, b.parent_hash)); }
                         persistence_content = Some(content);
                         let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
                         self.last_seq_head_move_time.store(now_ms, Ordering::SeqCst);
                         info!("[CHAIN] Gap filled: {}", num);
                    } else {
                        if chain.iter().any(|b| b.hash == hash_hex) { return BlockUpdateResult::Duplicate; }
                    }
                }
            }
        } 

        if let Some(content) = persistence_content {
            let _ = std::fs::write(STATE_FILE, content);
            info!("[CHAIN] Tip: {}", num);
        }
        result
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
        for addr in addrs {
            if let Some(mut subs) = self.subscribers.get_mut(*addr) {
                if !subs.is_empty() {
                    found = true;
                    subs.retain(|(_, s)| s.try_send(event.clone()).is_ok());
                }
            }
        }
        found
    }
}
