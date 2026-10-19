use dotenvy::dotenv;
use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub network: String,
    pub p2p_version: i32,
    pub seed_nodes: Vec<String>,
    pub node_id: Vec<u8>,
    pub external_ip: String,
    pub node_grpc_url: String,
    pub grpc_listen_addr: String,
}

pub const DEFAULT_BLOCK_NUM: u64 = 81_728_607;
pub const DEFAULT_BLOCK_HASH: &str = "0000000004df145fc80be5639ed7a12fd282bf1591be3998806a9290708c893d";

impl Config {
    pub fn load() -> Self {
        dotenv().ok();

        let network = env::var("NETWORK").unwrap_or_else(|_| "mainnet".to_string());
        let p2p_version = env::var("P2P_VERSION")
            .unwrap_or_else(|_| "11111".to_string())
            .parse()
            .unwrap_or(11111);

        let seed_nodes_env = env::var("SEED_NODES").unwrap_or_default();
        let seed_nodes = if !seed_nodes_env.is_empty() {
            seed_nodes_env
                .split(',')
                .map(|s| s.trim().to_string())
                .collect()
        } else {
            crate::seed_nodes::SEED_NODES
                .iter()
                .map(|&s| s.to_string())
                .collect()
        };

        let node_id = env::var("NODE_ID").unwrap_or_default();
        let node_id = if !node_id.is_empty() {
            hex::decode(node_id).unwrap_or_else(|_| vec![0u8; 64])
        } else {
            use rand::Rng;
            let mut rng = rand::thread_rng();
            let mut id = vec![0u8; 64];
            rng.fill(&mut id[..]);
            id
        };

        let external_ip = env::var("EXTERNAL_IP").unwrap_or_else(|_| "127.0.0.1".to_string());
        let node_grpc_url = env::var("NODE_GRPC_URL").unwrap_or_else(|_| "tron.coinductor.io:50051".to_string());
        let grpc_listen_addr = env::var("GRPC_LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:50051".to_string());

        Config {
            network,
            p2p_version,
            seed_nodes,
            node_id,
            external_ip,
            node_grpc_url,
            grpc_listen_addr,
        }
    }
}
