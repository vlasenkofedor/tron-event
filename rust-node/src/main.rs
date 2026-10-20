use rust_node::{config, logging, state, p2p, api};
use config::Config;
use state::NodeState;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{debug, error, info};

/// Maximum concurrent outbound P2P connections.
const MAX_CONCURRENT_PEERS: usize = 5;
/// Delay between connection attempts to the same peer (ms).
const CONNECTION_STAGGER_MS: u64 = 500;
/// Delay between full seed-node rotation cycles (s).
const ROTATION_DELAY_SECS: u64 = 30;

/// Tron Event Node — high-performance P2P transaction monitor.
///
/// Starts gRPC API server, status worker, P2P listener, and outbound
/// connection manager. Runs indefinitely until process is terminated.
#[tokio::main]
async fn main() {
    // 1. Initialize logging
    logging::init_logging();

    // 2. Load configuration
    let cfg = Arc::new(Config::load());
    info!(
        "Starting Rust Tron Node... Network: {}, P2P Version: {}",
        cfg.network, cfg.p2p_version
    );

    // 3. Load / create persistent node state (last block + txid dedup)
    let node_state = Arc::new(NodeState::load());

    // 4. Start gRPC API Server
    let state_api = node_state.clone();
    let api_addr = cfg.grpc_listen_addr.clone();
    tokio::spawn(async move {
        if let Err(e) = api::start_api_server(state_api, api_addr).await {
            error!("gRPC API server error: {}", e);
        }
    });

    // 5. Start Transaction Status Worker (confirmation / finality)
    let state_status = node_state.clone();
    tokio::spawn(async move {
        p2p::status_worker::start_status_worker(state_status).await;
    });

    // 6. Start P2P Inbound Listener (passive — let peers connect to us)
    let state_listener = node_state.clone();
    let cfg_listener = cfg.clone();
    tokio::spawn(async move {
        p2p::accept_connections(cfg_listener.as_ref(), state_listener).await;
    });

    // 7. Background Cleanup Task (Prune stale tracked transactions)
    let state_cleanup = node_state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(600)); // Every 10 mins
        loop {
            interval.tick().await;
            state_cleanup.prune_tracked_txs(std::time::Duration::from_secs(3600)); // Remove older than 1h
        }
    });

    // 8. Graceful shutdown handler
    let shutdown = tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => info!("Shutdown signal received, stopping..."),
            Err(e) => error!("Failed to listen for ctrl_c: {}", e),
        }
    });

    // 9. Start P2P Parallel Connection Manager (outbound)
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_PEERS));
    let cfg_manager = cfg.clone();
    let state_manager = node_state.clone();

    tokio::spawn(async move {
        loop {
            let mut nodes = cfg_manager.seed_nodes.clone();
            {
                use rand::seq::SliceRandom;
                let mut rng = rand::thread_rng();
                nodes.shuffle(&mut rng);
            }

            for node in nodes {
                let permit = semaphore.clone().acquire_owned().await.expect("semaphore not closed");
                let cfg_peer = cfg_manager.clone();
                let state_peer = state_manager.clone();
                let peer_addr = node.clone();

                tokio::spawn(async move {
                    debug!("Attempting parallel connection to {}...", peer_addr);
                    let worker_cfg = cfg_peer.as_ref().clone();
                    match p2p::connect_to_node(&worker_cfg, &peer_addr, state_peer).await {
                        Ok(_) => debug!("Session with {} ended cleanly", peer_addr),
                        Err(e) => {
                            if !e.to_string().contains("Already connected") && !e.to_string().contains("timed out") {
                                error!("Connection to {} error: {}", peer_addr, e);
                            }
                        }
                    }
                    drop(permit);
                });
                
                // Stagger to avoid rate-limiting/IP bans
                tokio::time::sleep(std::time::Duration::from_millis(CONNECTION_STAGGER_MS)).await;
            }
            
            info!("Finished cycling through all seed nodes. Waiting before next rotation...");
            tokio::time::sleep(std::time::Duration::from_secs(ROTATION_DELAY_SECS)).await;
        }
    });

    // Wait for shutdown signal
    let _ = shutdown.await;
    info!("Node stopped.");
}
