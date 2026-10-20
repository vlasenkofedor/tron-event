pub mod handlers;
pub mod tx_processor;
pub mod utils;
pub mod status_worker;

use crate::config::Config;
use crate::error::Result;
use crate::net::{P2pMessage, TronCodec};
use crate::protocol;
use crate::state::NodeState;
use handlers::{PeerState, WriterChannel};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::{TcpStream, TcpListener, tcp::{OwnedReadHalf, OwnedWriteHalf}};
use tracing::{info, warn};
use utils::ip_to_bytes;
use std::sync::atomic::Ordering;

/// Spawn a writer task that owns the TCP write half and receives messages via channel.
///
/// Returns an `UnboundedSender` that callers use to queue messages for writing.
/// The writer task runs until the channel is closed (all senders dropped).
async fn spawn_writer_task(
    writer: OwnedWriteHalf,
    peer_addr: String,
) -> WriterChannel {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(P2pMessage, bool)>();
    tokio::spawn(async move {
        let mut writer = writer;
        while let Some((msg, compressed)) = rx.recv().await {
            if let Err(e) = TronCodec::write_message(&mut writer, &msg, compressed, &peer_addr).await {
                warn!("[{}] Writer task error: {}", peer_addr, e);
                break;
            }
        }
        info!("[{}] Writer task exited", peer_addr);
    });
    tx
}

pub async fn accept_connections(cfg: &Config, state: Arc<NodeState>) {
    let bind_addr = format!("0.0.0.0:18888");
    let listener = match TcpListener::bind(&bind_addr).await {
        Ok(l) => { info!("P2P listener started on {}", bind_addr); l }
        Err(e) => { tracing::error!("Failed to bind P2P listener on {}: {}", bind_addr, e); return; }
    };
    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                let addr_str = peer_addr.to_string();
                if state.active_peers.contains_key(&addr_str) { continue; }
                state.active_peers.insert(addr_str.clone(), ());
                let cfg_arc = Arc::new(cfg.clone());
                let state_clone = state.clone();
                tokio::spawn(async move {
                    info!("[{}] Inbound connection accepted", addr_str);
                    let mut tx_rx = state_clone.tx_broadcast.subscribe();
                    match run_inbound_connection(addr_str.clone(), stream, cfg_arc, state_clone.clone(), &mut tx_rx).await {
                        Ok(_) => {}
                        Err(e) => tracing::error!("[{}] Inbound connection error: {}", addr_str, e),
                    }
                    state_clone.active_peers.remove(&addr_str);
                });
            }
            Err(e) => tracing::error!("Accept error: {}", e),
        }
    }
}

async fn run_inbound_connection(
    peer_addr: String,
    stream: TcpStream,
    cfg: Arc<Config>,
    state: Arc<NodeState>,
    tx_rx: &mut tokio::sync::broadcast::Receiver<protocol::protocol::Transaction>,
) -> Result<()> {
    let (reader, writer) = stream.into_split();
    let writer_tx = spawn_writer_task(writer, peer_addr.clone()).await;
    let mut peer_state = PeerState::new();
    let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0);
    let mut reader = reader;

    tokio::time::timeout(tokio::time::Duration::from_secs(20), async {
        while !peer_state.tron_hello_recvd {
            let res = TronCodec::read_message(&mut reader, peer_state.compression_enabled, &peer_addr).await?;
            match res {
                P2pMessage::Libp2pHello(h) => {
                    info!("[{}] Inbound: Received LibP2P Hello (v={}, net={})", peer_addr, h.version, h.network_id);
                    let libp2p_hello = protocol::p2p::HelloMessage {
                        from: Some(protocol::protocol::Endpoint { address: ip_to_bytes(&cfg.external_ip), port: 18888, node_id: cfg.node_id.clone(), address_ipv6: vec![] }),
                        network_id: 11111, code: 0, timestamp: now, version: 1,
                    };
                    writer_tx.send((P2pMessage::Libp2pHello(libp2p_hello), false)).map_err(|_| crate::error::NodeError::Network("writer channel closed".to_string()))?;

                    let (cur_num, cur_hash_hex) = state.last_block();
                    // The hash must be a full 32-byte block id. Advertising an
                    // empty hash makes the peer reject the handshake with
                    // INCOMPATIBLE_PROTOCOL.
                    let cur_hash = hex::decode(&cur_hash_hex).unwrap_or_default();
                    // `cfg.node_id` already holds the raw bytes; decoding it
                    // again as hex yields an empty node id, which makes peers
                    // reject the handshake.
                    let tron_hello = protocol::protocol::HelloMessage {
                        from: Some(protocol::protocol::Endpoint { address: ip_to_bytes(&cfg.external_ip), port: 18888, node_id: cfg.node_id.clone(), ..Default::default() }),
                        version: cfg.p2p_version as i32, timestamp: now,
                        genesis_block_id: Some(protocol::protocol::hello_message::BlockId { hash: hex::decode(crate::state::GENESIS_HASH).unwrap_or_default(), number: 0 }),
                        solid_block_id: Some(protocol::protocol::hello_message::BlockId { hash: cur_hash.clone(), number: cur_num as i64 }),
                        head_block_id: Some(protocol::protocol::hello_message::BlockId { hash: cur_hash, number: cur_num as i64 }),
                        address: vec![], code_version: b"4.8.1".to_vec(), node_type: 1,
                        lowest_block_num: cur_num as i64, ..Default::default()
                    };
                    writer_tx.send((P2pMessage::TronHello(tron_hello), false)).map_err(|_| crate::error::NodeError::Network("writer channel closed".to_string()))?;
                }
                P2pMessage::Libp2pDisconnect(_) | P2pMessage::TronDisconnect(_) => return Err(crate::error::NodeError::Network("Peer disconnected during handshake".to_string())),
                other => { handlers::handle_message(other, &peer_addr, &writer_tx, &state, &mut peer_state).await?; }
            }
        }
        Ok::<(), crate::error::NodeError>(())
    }).await.map_err(|_| crate::error::NodeError::Network("Inbound handshake timed out".to_string()))??;

    run_p2p_loop(peer_addr, reader, writer_tx, state, &mut peer_state, tx_rx).await
}

pub async fn connect_to_node(cfg: &Config, addr: &str, state: Arc<NodeState>) -> Result<()> {
    if state.active_peers.contains_key(addr) { return Ok(()); }
    state.active_peers.insert(addr.to_string(), ());
    let cfg_arc = Arc::new(cfg.clone());
    let state_clone = state.clone();
    let addr_str = addr.to_string();
    let res = tokio::spawn(async move {
        // A connect timeout is essential: a peer that accepts the TCP
        // handshake but never completes the protocol handshake would
        // otherwise hold the connection slot forever. With a limited number
        // of slots, a few unresponsive seeds would block all outgoing
        // connections and the node would stop syncing entirely.
        let stream = match tokio::time::timeout(
            std::time::Duration::from_secs(10),
            TcpStream::connect(&addr_str),
        )
        .await
        {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                return Err(crate::error::NodeError::Network(format!(
                    "Connection failed to {}: {}",
                    addr_str, e
                )));
            }
            Err(_) => {
                return Err(crate::error::NodeError::Network(format!(
                    "Connection timeout to {}",
                    addr_str
                )));
            }
        };
        info!("[{}] Connected, starting handshake", addr_str);
        let mut tx_rx = state_clone.tx_broadcast.subscribe();
        run_connection(addr_str.clone(), stream, cfg_arc, state_clone.clone(), &mut tx_rx).await
    }).await;
    state.active_peers.remove(addr);
    match res { Ok(r) => r, Err(e) => Err(crate::error::NodeError::Network(format!("Task panic: {}", e))) }
}

async fn run_connection(
    peer_addr: String,
    stream: TcpStream,
    cfg: Arc<Config>,
    state: Arc<NodeState>,
    tx_rx: &mut tokio::sync::broadcast::Receiver<protocol::protocol::Transaction>,
) -> Result<()> {
    let (reader, writer) = stream.into_split();
    let writer_tx = spawn_writer_task(writer, peer_addr.clone()).await;
    let mut peer_state = PeerState::new();
    let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0);
    let libp2p_hello = protocol::p2p::HelloMessage {
        from: Some(protocol::protocol::Endpoint { address: ip_to_bytes(&cfg.external_ip), port: 18888, node_id: cfg.node_id.clone(), address_ipv6: vec![] }),
        network_id: 11111, code: 0, timestamp: now, version: 1, 
    };
    writer_tx.send((P2pMessage::Libp2pHello(libp2p_hello), false)).map_err(|_| crate::error::NodeError::Network("writer channel closed".to_string()))?;
    let mut reader = reader;
    tokio::time::timeout(tokio::time::Duration::from_secs(20), async {
        while !peer_state.tron_hello_recvd {
            let res = TronCodec::read_message(&mut reader, peer_state.compression_enabled, &peer_addr).await?;
            match res {
                P2pMessage::Libp2pHello(h) => {
                    info!("[{}] Handshake: Received LibP2P Hello (v={}, net={})", peer_addr, h.version, h.network_id);
                    let (cur_num, cur_hash_hex) = state.last_block();
                    // The hash must be a full 32-byte block id. Advertising an
                    // empty hash makes the peer reject the handshake with
                    // INCOMPATIBLE_PROTOCOL.
                    let cur_hash = hex::decode(&cur_hash_hex).unwrap_or_default();
                    // `cfg.node_id` already holds the raw bytes; decoding it
                    // again as hex yields an empty node id, which makes peers
                    // reject the handshake.
                    let tron_hello = protocol::protocol::HelloMessage {
                        from: Some(protocol::protocol::Endpoint { address: ip_to_bytes(&cfg.external_ip), port: 18888, node_id: cfg.node_id.clone(), ..Default::default() }),
                        version: cfg.p2p_version as i32, timestamp: now,
                        genesis_block_id: Some(protocol::protocol::hello_message::BlockId { hash: hex::decode(crate::state::GENESIS_HASH).unwrap_or_default(), number: 0 }),
                        solid_block_id: Some(protocol::protocol::hello_message::BlockId { hash: cur_hash.clone(), number: cur_num as i64 }),
                        head_block_id: Some(protocol::protocol::hello_message::BlockId { hash: cur_hash, number: cur_num as i64 }),
                        address: vec![], code_version: b"4.8.1".to_vec(), node_type: 1,
                        lowest_block_num: cur_num as i64, ..Default::default()
                    };
                    writer_tx.send((P2pMessage::TronHello(tron_hello), false)).map_err(|_| crate::error::NodeError::Network("writer channel closed".to_string()))?;
                }
                P2pMessage::Libp2pDisconnect(_) | P2pMessage::TronDisconnect(_) => return Err(crate::error::NodeError::Network("Peer disconnected during handshake".to_string())),
                other => { handlers::handle_message(other, &peer_addr, &writer_tx, &state, &mut peer_state).await?; }
            }
        }
        Ok::<(), crate::error::NodeError>(())
    }).await.map_err(|_| crate::error::NodeError::Network("Handshake timed out".to_string()))??;
    run_p2p_loop(peer_addr, reader, writer_tx, state, &mut peer_state, tx_rx).await
}

async fn run_p2p_loop(
    peer_addr: String,
    mut reader: OwnedReadHalf,
    writer_tx: WriterChannel,
    state: Arc<NodeState>,
    peer_state: &mut PeerState,
    tx_rx: &mut tokio::sync::broadcast::Receiver<protocol::protocol::Transaction>,
) -> Result<()> {
    let mut ping_task: Option<tokio::task::JoinHandle<()>> = None;
    let mut watchdog_interval = tokio::time::interval(tokio::time::Duration::from_secs(15));
    let mut sync_cmd_rx = state.sync_commands.subscribe();
    
    let res = loop {
        tokio::select! {
            _ = watchdog_interval.tick() => {
                let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0);

                // Detect a stalled fetch using the time of the last request
                // we sent, not the last block we received: a peer may keep
                // sending other blocks while never delivering the one we
                // asked for, which would keep the stall invisible.
                if peer_state.pending_blocks > 0
                    && now.saturating_sub(peer_state.last_fetch_time) > 10_000
                {
                    warn!(
                        "[{}] Inventory fetch stalled for 10s. Resetting pending state to allow a new SyncBlockChain.",
                        peer_addr
                    );
                    peer_state.pending_blocks = 0;
                    peer_state.pending_inventory.clear();
                    // Forget which hashes were already requested: the peer
                    // never delivered them, so they must be requestable again.
                    // Without this the fetch loop deadlocks permanently.
                    peer_state.req_blocks.clear();
                }

                // Recycle a peer that stopped streaming blocks.
                //
                // A peer answers a sync request with a window of blocks and
                // then keeps streaming them. The node asks for the next
                // window itself, so a quiet peer is only recycled when the
                // chain stopped advancing altogether.
                //
                // The threshold is deliberately generous: dropping a
                // connection too eagerly makes java-tron peers blacklist the
                // node, which costs far more than waiting.
                if peer_state.last_block_recv_time > 0
                    && now.saturating_sub(peer_state.last_block_recv_time) > 45_000
                {
                    warn!(
                        "[{}] No blocks received for 45s. Recycling connection.",
                        peer_addr
                    );
                    break Err(crate::error::NodeError::Network("Block stream idle, recycling peer".to_string()));
                }

                let gaps = state.pending_gaps.lock().len();
                if gaps > 0 {
                    let last_move = state.last_seq_head_move_time.load(Ordering::SeqCst);
                    if now.saturating_sub(last_move) > 45_000 { // 45s threshold
                        warn!("[{}] RECYCLE: SeqHead stalled for 45s with {} gaps. Dropping peer to recycle connection.", peer_addr, gaps);
                        break Err(crate::error::NodeError::Network("Sync stall recycler".to_string()));
                    }
                }
            }
            Ok(sync_req) = sync_cmd_rx.recv() => {
                // Forward the periodic sync request to this peer.
                //
                // `status_worker` broadcasts a sync request every few
                // seconds once the chain stops advancing. Repeated requests
                // on one connection are accepted, but a burst makes the peer
                // drop the connection, so each peer is asked at most once
                // per 3 seconds and only while it has no fetch in flight.
                let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0);
                if peer_state.pending_blocks == 0
                    && now.saturating_sub(peer_state.last_sync_request_time) >= 3000
                {
                    peer_state.last_sync_request_time = now;
                    let _ = writer_tx.send((
                        P2pMessage::TronSyncBlockChain(sync_req),
                        peer_state.compression_enabled,
                    ));
                }
            }
            Ok(tx) = tx_rx.recv() => {
                let _ = writer_tx.send((P2pMessage::TronTrx(tx), peer_state.compression_enabled));
            }
            res = TronCodec::read_message(&mut reader, peer_state.compression_enabled, &peer_addr) => {
                let msg = match res { Ok(m) => m, Err(e) => break Err(e), };
                match msg {
                    P2pMessage::TronPing(data) => {
                        let _ = writer_tx.send((P2pMessage::TronPong(data), peer_state.compression_enabled));
                    }
                    other => {
                        handlers::handle_message(other, &peer_addr, &writer_tx, &state, peer_state).await?;
                        if ping_task.is_none() {
                            let w_tx = writer_tx.clone();
                            ping_task = Some(tokio::spawn(async move {
                                loop {
                                    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                                    let _ = w_tx.send((P2pMessage::TronPing(vec![0xC0]), false));
                                }
                            }));
                        }
                    }
                }
            }
        }
    };
    if let Some(t) = ping_task { t.abort(); }
    res
}
