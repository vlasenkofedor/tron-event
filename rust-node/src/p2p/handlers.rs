use crate::net::{P2pMessage, TronCodec};
use crate::p2p::tx_processor;
use crate::p2p::utils::protocol_transaction_id;
use crate::protocol;
use crate::state::{NodeState, BlockUpdateResult};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::net::tcp::OwnedWriteHalf;
use tracing::{info, warn, debug};
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};
use lru::LruCache;
use std::collections::VecDeque;

pub struct PeerState {
    pub compression_enabled: bool,
    pub sync_sent: bool,
    pub tron_hello_recvd: bool,
    pub req_blocks: LruCache<Vec<u8>, ()>,
    pub pending_inventory: VecDeque<Vec<u8>>,
    pub pending_blocks: usize,
    pub last_block_recv_time: u64,
}

pub async fn handle_message(
    msg: P2pMessage,
    peer_addr: &str,
    writer: &Arc<Mutex<OwnedWriteHalf>>,
    state: &Arc<NodeState>,
    peer_state: &mut PeerState,
) -> crate::error::Result<()> {
    match msg {
        P2pMessage::TronHello(h) => handle_tron_hello(h, peer_addr, writer, state, peer_state).await,
        P2pMessage::TronBlockChainInventory(inv) => handle_chain_inventory(inv, peer_addr, writer, peer_state, state).await,
        P2pMessage::TronSyncBlockChain(inv) => handle_sync_block_chain(inv, peer_addr, writer, peer_state).await,
        P2pMessage::TronInventory(inv) => handle_inventory(inv, peer_addr, writer, peer_state, state).await,
        P2pMessage::TronTrxsInventory(inv) => handle_trxs_inventory(inv, peer_addr, writer, peer_state).await,
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
            let _ = TronCodec::write_message(&mut *writer.lock().await, &P2pMessage::TronPong(data), peer_state.compression_enabled, peer_addr).await;
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
    writer: &Arc<Mutex<OwnedWriteHalf>>,
    state: &Arc<NodeState>,
    peer_state: &mut PeerState,
) -> crate::error::Result<()> {
    let gen_hash = h.genesis_block_id.as_ref().map(|id| hex::encode(&id.hash)).unwrap_or_default();
    info!("[{}] Received Tron Hello (head={}, genesis={})", peer_addr, h.head_block_id.as_ref().map(|id| id.number).unwrap_or(0), gen_hash);
    peer_state.tron_hello_recvd = true;
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
        TronCodec::write_message(&mut *writer.lock().await, &P2pMessage::TronSyncBlockChain(sync_req), false, peer_addr).await?;
        peer_state.sync_sent = true;
    }
    Ok(())
}

pub async fn fetch_next_batch(
    peer_state: &mut PeerState,
    writer: &Arc<Mutex<OwnedWriteHalf>>,
    peer_addr: &str,
) -> crate::error::Result<()> {
    let mut ids_to_fetch = Vec::new();
    while let Some(hash) = peer_state.pending_inventory.pop_front() {
        if !peer_state.req_blocks.contains(&hash) {
            peer_state.req_blocks.put(hash.clone(), ());
            ids_to_fetch.push(hash);
            if ids_to_fetch.len() >= 100 {
                break;
            }
        }
    }
    
    if !ids_to_fetch.is_empty() {
        peer_state.pending_blocks += ids_to_fetch.len();
        let fetch = protocol::protocol::Inventory { r#type: 1, ids: ids_to_fetch.clone() }; 
        info!("[{}] FETCH: Requesting {} block(s), {} remaining in inventory queue", peer_addr, ids_to_fetch.len(), peer_state.pending_inventory.len());
        TronCodec::write_message(&mut *writer.lock().await, &P2pMessage::TronFetchInvData(fetch), peer_state.compression_enabled, peer_addr).await?;
    }
    Ok(())
}

async fn handle_chain_inventory(
    inv: protocol::protocol::ChainInventory,
    peer_addr: &str,
    writer: &Arc<Mutex<OwnedWriteHalf>>,
    peer_state: &mut PeerState,
    _state: &Arc<NodeState>,
) -> crate::error::Result<()> {
    info!("[{}] Received ChainInventory (0x09) with {} hashes", peer_addr, inv.ids.len());
    peer_state.pending_inventory.clear();
    for id in inv.ids.into_iter() {
        peer_state.pending_inventory.push_back(id.hash);
    }
    
    if peer_state.pending_blocks == 0 {
        fetch_next_batch(peer_state, writer, peer_addr).await?;
    }
    Ok(())
}

async fn handle_sync_block_chain(
    _inv: protocol::protocol::BlockInventory,
    peer_addr: &str,
    writer: &Arc<Mutex<OwnedWriteHalf>>,
    peer_state: &PeerState,
) -> crate::error::Result<()> {
    let resp = protocol::protocol::ChainInventory { ids: vec![], remain_num: 0 };
    TronCodec::write_message(&mut *writer.lock().await, &P2pMessage::TronBlockChainInventory(resp), peer_state.compression_enabled, peer_addr).await?;
    Ok(())
}

async fn handle_inventory(
    inv: protocol::protocol::Inventory,
    peer_addr: &str,
    writer: &Arc<Mutex<OwnedWriteHalf>>,
    peer_state: &PeerState,
    state: &Arc<NodeState>,
) -> crate::error::Result<()> {
    debug!("[{}] Received Inventory (0x01) with {} items, type={}", peer_addr, inv.ids.len(), inv.r#type);
    if inv.r#type == 0 {
        state.stats_inventory_tx.fetch_add(inv.ids.len() as u64, Ordering::Relaxed);
    }
    TronCodec::write_message(&mut *writer.lock().await, &P2pMessage::TronFetchInvData(inv), peer_state.compression_enabled, peer_addr).await?;
    Ok(())
}

async fn handle_trxs_inventory(
    inv: protocol::protocol::BlockInventory,
    peer_addr: &str,
    writer: &Arc<Mutex<OwnedWriteHalf>>,
    peer_state: &PeerState,
) -> crate::error::Result<()> {
    let ids = inv.ids.iter().map(|id| id.hash.clone()).collect();
    let fetch = protocol::protocol::Inventory { r#type: 0, ids };
    TronCodec::write_message(&mut *writer.lock().await, &P2pMessage::TronFetchInvData(fetch), peer_state.compression_enabled, peer_addr).await?;
    Ok(())
}

pub async fn handle_block(
    block: protocol::protocol::Block,
    state: &Arc<NodeState>,
    writer: &Arc<Mutex<OwnedWriteHalf>>,
    peer_state: &mut PeerState,
    peer_addr: &str,
) -> crate::error::Result<()> {
    peer_state.last_block_recv_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    
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
        let update_result = state.update_block(header.number as u64, &block_id, &header.parent_hash);
        
        match update_result {
            BlockUpdateResult::GapDetected { missing_count: _, last_good_hash: _ } => {
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
                let last = state.last_sync_request_time.load(Ordering::SeqCst);
                if now.saturating_sub(last) > 3000 {
                    state.last_sync_request_time.store(now, Ordering::SeqCst);
                    let locators = state.get_locators(10);
                    let ids = locators.iter().map(|(num, hash_hex)| {
                        protocol::protocol::block_inventory::BlockId { hash: hex::decode(hash_hex).unwrap_or_default(), number: *num as i64 }
                    }).collect();
                    let sync_req = protocol::protocol::BlockInventory { ids, r#type: 0 };
                    let _ = TronCodec::write_message(&mut *writer.lock().await, &P2pMessage::TronSyncBlockChain(sync_req), peer_state.compression_enabled, peer_addr).await;
                }
            }
            BlockUpdateResult::Duplicate => { return Ok(()); }
            BlockUpdateResult::Success => {}
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
