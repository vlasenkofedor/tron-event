use rust_node::{config, logging, state, p2p, api};
use config::Config;
use state::NodeState;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{debug, error, info};

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

    // 5. Start Transaction Status Worker (Confirmation / Solidity)
    let state_status = node_state.clone();
    let grpc_url = cfg.node_grpc_url.clone();
    tokio::spawn(async move {
        p2p::status_worker::start_status_worker(state_status, grpc_url).await;
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

    // 7. Start P2P Parallel Connection Manager (outbound)
    let semaphore = Arc::new(Semaphore::new(5)); // Max 5 simultaneous connections to avoid IP bans
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
                let permit = semaphore.clone().acquire_owned().await.unwrap();
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
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            
            info!("Finished cycling through all seed nodes. Waiting before next rotation...");
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        }
    });

    // Keep main thread alive
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}
