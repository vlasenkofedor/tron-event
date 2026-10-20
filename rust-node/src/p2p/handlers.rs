use crate::net::P2pMessage;
use crate::p2p::tx_processor;
use crate::p2p::utils::protocol_transaction_id;
use crate::protocol;
use crate::state::{NodeState, BlockUpdateResult};
use std::sync::Arc;
use tracing::{info, warn, debug};
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};
use lru::LruCache;
use std::collections::VecDeque;

/// Channel for sending P2P messages to the writer task.
pub type WriterChannel = tokio::sync::mpsc::UnboundedSender<(P2pMessage, bool)>;

pub struct PeerState {
    pub compression_enabled: bool,
    pub sync_sent: bool,
    pub tron_hello_recvd: bool,
    pub req_blocks: LruCache<Vec<u8>, ()>,
    pub pending_inventory: VecDeque<Vec<u8>>,
    pub pending_blocks: usize,
    pub last_block_recv_time: u64,
    /// Timestamp (ms) of the last FetchInvData request sent to this peer.
    ///
    /// Used to detect a stalled fetch independently of other traffic: a peer
    /// may keep delivering blocks while never delivering the specific block
    /// we asked for.
    pub last_fetch_time: u64,
    /// Timestamp (ms) of the last SyncBlockChain request sent to this peer.
    ///
    /// A peer answers a sync request with a window of blocks. Asking again
    /// immediately after consuming that window is required to make progress,
    /// but a burst of requests makes the peer drop the connection, so the
    /// requests are throttled.
    pub last_sync_request_time: u64,
    /// Transaction hashes already requested from this peer, with the time of
    /// the request.
    ///
    /// Used to avoid re-requesting the same ids in a burst. The entries are
    /// forgotten after a short while, because a peer re-announces the same
    /// ids and a permanently blocked id would never be fetched.
    pub req_txs: LruCache<Vec<u8>, u64>,
}

impl PeerState {
    /// Create a fresh peer state with default tracking values.
    pub fn new() -> Self {
        Self {
            compression_enabled: false,
            sync_sent: false,
            tron_hello_recvd: false,
            req_blocks: LruCache::new(std::num::NonZeroUsize::new(20000).unwrap()),
            pending_inventory: VecDeque::new(),
            pending_blocks: 0,
            last_block_recv_time: 0,
            last_fetch_time: 0,
            last_sync_request_time: 0,
            req_txs: LruCache::new(std::num::NonZeroUsize::new(20000).unwrap()),        }
    }
}

impl Default for PeerState {
    fn default() -> Self {
        Self::new()
    }
}

/// Dispatch a P2P message to its specific handler.
///
/// Routes messages by type: Hello, Inventory, Block, Transaction, Disconnect, etc.
/// Spawns async tasks for transaction processing with semaphore-bounded concurrency.
pub async fn handle_message(
    msg: P2pMessage,
    peer_addr: &str,
    writer: &WriterChannel,
    state: &Arc<NodeState>,
    peer_state: &mut PeerState,
) -> crate::error::Result<()> {
    match msg {
        P2pMessage::TronHello(h) => handle_tron_hello(h, peer_addr, writer, state, peer_state).await,
        P2pMessage::TronBlockChainInventory(inv) => handle_chain_inventory(inv, peer_addr, writer, peer_state, state).await,
        P2pMessage::TronSyncBlockChain(inv) => handle_sync_block_chain(inv, peer_addr, writer, peer_state).await,
        P2pMessage::TronInventory(inv) => handle_inventory(inv, peer_addr, writer, peer_state, state).await,
        P2pMessage::TronTrxsInventory(inv) => handle_trxs_inventory(inv, peer_addr, writer, peer_state).await,
        P2pMessage::TronBlockInventory(inv) => handle_block_inventory(inv, peer_addr, writer, peer_state).await,
        P2pMessage::TronBlock(block) => handle_block(block, state, writer, peer_state, peer_addr).await,
        P2pMessage::TronTrx(tx) => {
            let tid = protocol_transaction_id(&tx);
            if state.is_new_tx(&tid) {
                let s_p = state.clone();
                tokio::spawn(async move {
                    let _permit = s_p.parsing_semaphore.acquire().await;
                    tx_processor::process_transaction(&tx, "broadcast", &s_p, None);
                });
            }
            Ok(())
        }
        P2pMessage::TronTrxs(tx_list) => {
            for tx in tx_list.transactions {
                let tid = protocol_transaction_id(&tx);
                if state.is_new_tx(&tid) {
                    let s_p = state.clone();
                    tokio::spawn(async move {
                        let _permit = s_p.parsing_semaphore.acquire().await;
                        tx_processor::process_transaction(&tx, "broadcast", &s_p, None);
                    });
                }
            }
            Ok(())
        }
        P2pMessage::TronDisconnect(d) => {
            warn!("[{}] Peer disconnect code={:?}", peer_addr, d.reason);
            Err(crate::error::NodeError::Network("Peer disconnected".to_string()))
        }
        P2pMessage::Libp2pDisconnect(_) => Err(crate::error::NodeError::Network("LibP2P disconnect".to_string())),
        P2pMessage::TronPing(data) => {
            let _ = writer.send((P2pMessage::TronPong(data), peer_state.compression_enabled));
            Ok(())
        }
        P2pMessage::Unknown(tid, data) => {
            info!("[{}] Received Unknown message type: 0x{:02x} (len={})", peer_addr, tid, data.len());
            Ok(())
        }
        _ => Ok(()),
    }
}

async fn handle_tron_hello(
    h: protocol::protocol::HelloMessage,
    peer_addr: &str,
    writer: &WriterChannel,
    state: &Arc<NodeState>,
    peer_state: &mut PeerState,
) -> crate::error::Result<()> {
    let gen_hash = h.genesis_block_id.as_ref().map(|id| hex::encode(&id.hash)).unwrap_or_default();
    let peer_head_hash = h.head_block_id.as_ref().map(|id| hex::encode(&id.hash)).unwrap_or_default();
    let peer_head_num = h.head_block_id.as_ref().map(|id| id.number).unwrap_or(0);
    info!("[{}] Received Tron Hello (head={}, genesis={})", peer_addr, peer_head_num, gen_hash);
    tracing::debug!("[{}] Peer head hash: {}", peer_addr, peer_head_hash);
    peer_state.tron_hello_recvd = true;
    // Re-anchor to the peer's head if our local chain position is stale.
    //
    // The node persists its chain position between restarts. If it has been
    // offline long enough, the persisted position can fall millions of blocks
    // behind the network. Peers only serve blocks from their own recent chain,
    // so a stale position can never be closed by normal block fetching and the
    // node would receive no transactions at all. Adopting the peer's validated
    // head block restores syncing immediately.
    if let Some(head) = h.head_block_id.as_ref() {
        if head.number > 0 {
            let head_hash = hex::encode(&head.hash);
            state.reanchor_to_head(head.number as u64, &head_hash);
        }
    }

    // Mainnet nodes usually expect compression if they sent it in their Hello
    // We detect it from the IN: type_id=0x20 line in codec, but let's be safe.
    // If h.node_type is full node, they definitely support it.

    if !peer_state.sync_sent {
        let locators = state.get_locators(5);
        let ids = locators.iter().map(|(num, hash_hex)| {
            protocol::protocol::block_inventory::BlockId {
                hash: hex::decode(hash_hex).unwrap_or_default(),
                number: *num as i64,
            }
        }).collect();

        let sync_req = protocol::protocol::BlockInventory { ids, r#type: 0 };
        writer.send((P2pMessage::TronSyncBlockChain(sync_req), false)).map_err(|_| crate::error::NodeError::Network("writer channel closed".to_string()))?;
        peer_state.sync_sent = true;
    }
    Ok(())
}

pub async fn fetch_next_batch(
    peer_state: &mut PeerState,
    writer: &WriterChannel,
    peer_addr: &str,
) -> crate::error::Result<()> {
    let mut ids_to_fetch = Vec::new();
    while let Some(hash) = peer_state.pending_inventory.pop_front() {
        // Always request what the peer just offered.
        //
        // A previously requested hash is not skipped: if the peer offered it
        // again, the earlier request never arrived. Skipping it would leave a
        // permanent hole in the chain and the node could never advance.
        if !ids_to_fetch.contains(&hash) {
            peer_state.req_blocks.put(hash.clone(), ());
            ids_to_fetch.push(hash);
            if ids_to_fetch.len() >= 100 {
                break;
            }
        }
    }

    if !ids_to_fetch.is_empty() {
        peer_state.pending_blocks += ids_to_fetch.len();
        peer_state.last_fetch_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let fetch = protocol::protocol::Inventory { r#type: 1, ids: ids_to_fetch.clone() };
        tracing::debug!(
            "[{}] FETCH ids: {:?}",
            peer_addr,
            ids_to_fetch.iter().map(|h| hex::encode(h)).collect::<Vec<_>>()
        );
        info!("[{}] FETCH: Requesting {} block(s), {} remaining in inventory queue", peer_addr, ids_to_fetch.len(), peer_state.pending_inventory.len());
        writer.send((P2pMessage::TronFetchInvData(fetch), peer_state.compression_enabled)).map_err(|_| crate::error::NodeError::Network("writer channel closed".to_string()))?;
    }
    Ok(())
}

async fn handle_chain_inventory(
    inv: protocol::protocol::ChainInventory,
    peer_addr: &str,
    writer: &WriterChannel,
    peer_state: &mut PeerState,
    _state: &Arc<NodeState>,
) -> crate::error::Result<()> {
    tracing::debug!(
        "[{}] ChainInventory (0x09): {} ids, remain_num={}, ids={:?}",
        peer_addr, inv.ids.len(), inv.remain_num,
        inv.ids.iter().map(|i| hex::encode(&i.hash)).collect::<Vec<_>>()
    );
    info!("[{}] Received ChainInventory (0x09) with {} hashes", peer_addr, inv.ids.len());

    // The previous fetch batch has fully resolved (pending_blocks == 0).
    // Clear the "already requested" set so the same hashes can be requested
    // again: the peer may re-offer blocks that we already hold (for example
    // when our locator did not move the peer's view forward), and skipping
    // them would deadlock the sync loop forever.
    if peer_state.pending_blocks == 0 {
        peer_state.req_blocks.clear();
    }

    // Merge the new inventory into the pending queue instead of replacing
    // it. Replacing it drops blocks that were announced but not fetched yet,
    // which leaves a permanent hole in the chain.
    for id in inv.ids.into_iter() {
        if !peer_state.pending_inventory.contains(&id.hash) {
            peer_state.pending_inventory.push_back(id.hash);
        }
    }

    // Always try to fetch: if a batch is still in flight the remaining ids
    // stay queued and are picked up as soon as it completes.
    if peer_state.pending_blocks == 0 {
        fetch_next_batch(peer_state, writer, peer_addr).await?;
    }
    Ok(())
}

async fn handle_sync_block_chain(
    _inv: protocol::protocol::BlockInventory,
    _peer_addr: &str,
    writer: &WriterChannel,
    peer_state: &PeerState,
) -> crate::error::Result<()> {
    let resp = protocol::protocol::ChainInventory { ids: vec![], remain_num: 0 };
    writer.send((P2pMessage::TronBlockChainInventory(resp), peer_state.compression_enabled)).map_err(|_| crate::error::NodeError::Network("writer channel closed".to_string()))?;
    Ok(())
}

async fn handle_inventory(
    inv: protocol::protocol::Inventory,
    peer_addr: &str,
    writer: &WriterChannel,
    peer_state: &mut PeerState,
    state: &Arc<NodeState>,
) -> crate::error::Result<()> {
    debug!("[{}] Received Inventory (0x06) with {} items, type={}", peer_addr, inv.ids.len(), inv.r#type);
    if inv.r#type == 0 {
        state.stats_inventory_tx.fetch_add(inv.ids.len() as u64, Ordering::Relaxed);
    }

    // MSG_INVENTORY (0x06) announces ids the peer holds. The reply must
    // carry the SAME inventory type as the announcement:
    //
    //   type 0 = TRX   -> request the announced transactions (mempool)
    //   type 1 = BLOCK -> request the announced blocks
    //
    // Requesting blocks as type 0 (or transactions as type 1) is a protocol
    // violation and the peer drops the connection.
    //
    // Ids requested very recently are skipped to avoid a request storm, but
    // they become eligible again after a short while: a peer re-announces
    // the same ids, and an id that was never delivered would otherwise
    // never be fetched.
    if inv.ids.is_empty() {
        return Ok(());
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    const REQ_RETRY_MS: u64 = 5_000;

    let mut wanted: Vec<Vec<u8>> = Vec::new();
    for id in &inv.ids {
        let recently_requested = peer_state
            .req_txs
            .peek(id)
            .map(|&ts| now.saturating_sub(ts) < REQ_RETRY_MS)
            .unwrap_or(false);

        if !recently_requested {
            peer_state.req_txs.put(id.clone(), now);
            wanted.push(id.clone());
            if wanted.len() >= 100 {
                break;
            }
        }
    }

    if wanted.is_empty() {
        return Ok(());
    }

    let fetch = protocol::protocol::Inventory { r#type: inv.r#type, ids: wanted };
    writer.send((P2pMessage::TronFetchInvData(fetch), peer_state.compression_enabled)).map_err(|_| crate::error::NodeError::Network("writer channel closed".to_string()))?;
    Ok(())
}

async fn handle_trxs_inventory(
    inv: protocol::protocol::BlockInventory,
    _peer_addr: &str,
    writer: &WriterChannel,
    peer_state: &mut PeerState,
) -> crate::error::Result<()> {
    // MSG_TRX_INVENTORY (0x13) announces transaction hashes the peer holds.
    // Requesting them (inventory type 0 = TRX) is how a light node receives
    // mempool transactions before they are included in a block.
    //
    // Ids that were requested very recently are skipped to avoid a request
    // storm, but they become eligible again after a short while.
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    const REQ_RETRY_MS: u64 = 5_000;

    let mut wanted: Vec<Vec<u8>> = Vec::new();
    for id in &inv.ids {
        let recently_requested = peer_state
            .req_txs
            .peek(&id.hash)
            .map(|&ts| now.saturating_sub(ts) < REQ_RETRY_MS)
            .unwrap_or(false);

        if !recently_requested {
            peer_state.req_txs.put(id.hash.clone(), now);
            wanted.push(id.hash.clone());
            if wanted.len() >= 100 {
                break;
            }
        }
    }

    if wanted.is_empty() {
        return Ok(());
    }
    let fetch = protocol::protocol::Inventory { r#type: 0, ids: wanted };
    writer.send((P2pMessage::TronFetchInvData(fetch), peer_state.compression_enabled)).map_err(|_| crate::error::NodeError::Network("writer channel closed".to_string()))?;
    Ok(())
}

/// Handle a block inventory announcement (0x12).
///
/// Peers use this message to announce blocks they have produced. This is the
/// primary mechanism that keeps a light node's chain moving forward: without
/// requesting these blocks the node would only ever learn about new blocks
/// through a full chain re-sync.
async fn handle_block_inventory(
    inv: protocol::protocol::BlockInventory,
    peer_addr: &str,
    writer: &WriterChannel,
    peer_state: &PeerState,
) -> crate::error::Result<()> {
    let ids: Vec<Vec<u8>> = inv.ids.iter().map(|id| id.hash.clone()).collect();
    if ids.is_empty() {
        return Ok(());
    }
    debug!("[{}] Received BlockInventory (0x12) with {} block(s)", peer_addr, ids.len());
    // Inventory type 1 = BLOCK.
    let fetch = protocol::protocol::Inventory { r#type: 1, ids };
    writer.send((P2pMessage::TronFetchInvData(fetch), peer_state.compression_enabled)).map_err(|_| crate::error::NodeError::Network("writer channel closed".to_string()))?;
    Ok(())
}

pub async fn handle_block(
    block: protocol::protocol::Block,
    state: &Arc<NodeState>,
    writer: &WriterChannel,
    peer_state: &mut PeerState,
    peer_addr: &str,
) -> crate::error::Result<()> {
    peer_state.last_block_recv_time = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0);
    
    let block_num = block.block_header.as_ref().and_then(|h| h.raw_data.as_ref()).map(|r| r.number).unwrap_or(0);
    debug!("[{}] IN: Block #{}", peer_addr, block_num);

    state.stats_blocks.fetch_add(1, Ordering::Relaxed);
    
    if peer_state.pending_blocks > 0 {
        peer_state.pending_blocks -= 1;
        if peer_state.pending_blocks == 0 && !peer_state.pending_inventory.is_empty() {
            fetch_next_batch(peer_state, writer, peer_addr).await?;
        }
    }    
    if let Some(header) = block.block_header.as_ref().and_then(|h| h.raw_data.as_ref()) {
        let block_id = crate::p2p::utils::compute_block_id(header.number, header);
        tracing::debug!(
            "[{}] Block #{} id={} parent={} | tip={:?}",
            peer_addr, header.number, hex::encode(&block_id), hex::encode(&header.parent_hash),
            state.last_block()
        );
        let update_result = state.update_block(header.number as u64, &block_id, &header.parent_hash);
        
        match update_result {
            BlockUpdateResult::GapDetected { missing_count: _, last_good_hash: _ } => {
                let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0);
                let last = state.last_sync_request_time.load(Ordering::SeqCst);
                if now.saturating_sub(last) > 3000 {
                    state.last_sync_request_time.store(now, Ordering::SeqCst);
                    let locators = state.get_locators(10);
                    let ids = locators.iter().map(|(num, hash_hex)| {
                        protocol::protocol::block_inventory::BlockId { hash: hex::decode(hash_hex).unwrap_or_default(), number: *num as i64 }
                    }).collect();
                    let sync_req = protocol::protocol::BlockInventory { ids, r#type: 0 };
                    let _ = writer.send((P2pMessage::TronSyncBlockChain(sync_req), peer_state.compression_enabled));
                }
            }
            BlockUpdateResult::Duplicate => { return Ok(()); }
            BlockUpdateResult::Reorg { ancestor_number, orphaned_txs } => {                warn!(
                    "[{}] Fork detected: rolled back to ancestor #{} ({} orphaned txs). \
                     Requesting re-sync from canonical chain.",
                    peer_addr, ancestor_number, orphaned_txs
                );
                // Re-sync from the rolled-back position so the canonical
                // branch is fetched again.
                let locators = state.get_locators(10);
                let ids = locators.iter().map(|(num, hash_hex)| {
                    protocol::protocol::block_inventory::BlockId {
                        hash: hex::decode(hash_hex).unwrap_or_default(),
                        number: *num as i64,
                    }
                }).collect();
                let sync_req = protocol::protocol::BlockInventory { ids, r#type: 0 };
                let _ = writer.send((P2pMessage::TronSyncBlockChain(sync_req), peer_state.compression_enabled));
            }
            BlockUpdateResult::Success => {}
        }

        // Apply any blocks that arrived out of order and have now become
        // connectable to the chain.
        state.drain_buffer();

        // Ask for the next window of blocks.
        //
        // A peer answers a sync request with a window of blocks and then
        // keeps streaming them. Once the window is exhausted the peer goes
        // quiet, so a new request is needed to make progress.
        //
        // The request is not throttled: java-tron peers accept repeated
        // SyncBlockChain messages on the same connection and answer each one
        // with the next window of blocks.
        if peer_state.pending_blocks == 0 && peer_state.pending_inventory.is_empty() {
            let locators = state.get_locators(10);
            if !locators.is_empty() {
                let ids = locators.iter().map(|(num, hash_hex)| {
                    protocol::protocol::block_inventory::BlockId {
                        hash: hex::decode(hash_hex).unwrap_or_default(),
                        number: *num as i64,
                    }
                }).collect();
                let sync_req = protocol::protocol::BlockInventory { ids, r#type: 0 };
                peer_state.last_sync_request_time = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                let _ = writer.send((P2pMessage::TronSyncBlockChain(sync_req), peer_state.compression_enabled));
            }
        }
    }

    for tx in block.transactions {
        let tid = protocol_transaction_id(&tx);
        if state.is_new_tx(&tid) {
            let s_p = state.clone();
            tokio::spawn(async move {
                let _permit = s_p.parsing_semaphore.acquire().await;
                tx_processor::process_transaction(&tx, "confirmed", &s_p, Some(block_num as u64));
            });
        }
    }
    Ok(())
}
