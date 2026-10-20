use dotenvy::dotenv;
use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub network: String,
    pub p2p_version: i32,
    pub seed_nodes: Vec<String>,
    pub node_id: Vec<u8>,
    pub external_ip: String,
    pub grpc_listen_addr: String,
    /// Blocks that must be built on top of a transaction before it is
    /// reported as `SOLID`. Configurable with `--solid-depth`.
    pub solid_depth: u64,
}

/// Default number of blocks used to consider a transaction final.
///
/// Tron reaches irreversibility after 19 blocks; the default keeps one extra
/// block as a safety margin.
pub const DEFAULT_SOLID_DEPTH: u64 = 20;

/// Smallest accepted value for `--solid-depth`.
///
/// Zero would mark a transaction as final in the block that contains it,
/// which defeats the purpose of the SOLID state.
pub const MIN_SOLID_DEPTH: u64 = 1;

/// Largest accepted value for `--solid-depth`.
///
/// A depth above this is almost certainly a typo and would keep transactions
/// pending forever on any realistic chain.
pub const MAX_SOLID_DEPTH: u64 = 100_000;

/// Fallback chain height used only before the first peer contact.
///
/// The node overrides this position as soon as it receives a `TronHello` from
/// a peer (see `NodeState::reanchor_to_head`). Peers only serve blocks from
/// their own recent chain, so a position that is far behind can never be
/// closed by normal block fetching.
pub const DEFAULT_BLOCK_NUM: u64 = 87_248_000;
/// Hash placeholder for the fallback position; replaced on first peer contact.
pub const DEFAULT_BLOCK_HASH: &str = "00000000000000001ebf88508a03865c71d452e25f4d51194196a1d22b6653dc";

impl Config {
    /// Load configuration from the process environment and command line.
    ///
    /// Falls back to sensible defaults for all fields.
    /// Critical settings: NETWORK, GRPC_LISTEN_ADDR, SEED_NODES.
    pub fn load() -> Self {
        Self::load_from(std::env::args().skip(1))
    }

    /// Load configuration from the process environment and the given CLI
    /// arguments (without the program name).
    ///
    /// An invalid `--solid-depth` value prints an error and exits with code 2
    /// rather than starting with an unexpected finality threshold.
    pub fn load_from<I, S>(args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        dotenv().ok();

        let solid_depth = match parse_solid_depth(args) {
            Ok(depth) => depth,
            Err(err) => {
                eprintln!("error: {err}\n\n{}", solid_depth_usage());
                std::process::exit(2);
            }
        };

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
        let grpc_listen_addr = env::var("GRPC_LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:50051".to_string());

        Config {
            network,
            p2p_version,
            seed_nodes,
            node_id,
            external_ip,
            grpc_listen_addr,
            solid_depth,
        }
    }
}

/// Parse the finality depth from CLI arguments.
///
/// Accepted forms:
/// - `--solid-depth 30`
/// - `--solid-depth=30`
///
/// When the option is absent, [`DEFAULT_SOLID_DEPTH`] is returned. When it is
/// repeated, the last occurrence wins. Unrelated arguments are ignored so the
/// node can be started with extra flags from a process supervisor.
pub fn parse_solid_depth<I, S>(args: I) -> Result<u64, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut iter = args.into_iter();
    let mut value: Option<u64> = None;

    while let Some(arg) = iter.next() {
        let arg = arg.as_ref();
        if arg == "--solid-depth" {
            let raw = iter
                .next()
                .ok_or_else(|| "`--solid-depth` requires a value".to_string())?;
            value = Some(parse_depth_value(raw.as_ref())?);
        } else if let Some(raw) = arg.strip_prefix("--solid-depth=") {
            value = Some(parse_depth_value(raw)?);
        }
    }

    Ok(value.unwrap_or(DEFAULT_SOLID_DEPTH))
}

fn parse_depth_value(raw: &str) -> Result<u64, String> {
    let depth: u64 = raw.parse().map_err(|_| {
        format!("invalid value '{raw}' for --solid-depth: expected a positive integer")
    })?;

    if !(MIN_SOLID_DEPTH..=MAX_SOLID_DEPTH).contains(&depth) {
        return Err(format!(
            "--solid-depth must be between {MIN_SOLID_DEPTH} and {MAX_SOLID_DEPTH}, got {depth}"
        ));
    }

    Ok(depth)
}

/// Usage text shown for `--help` and for invalid CLI values.
#[must_use]
pub fn solid_depth_usage() -> &'static str {
    "Usage: rust-node [OPTIONS]\n\n\
     Options:\n  \
     --solid-depth <N>  Blocks built on top of a transaction before it is\n                     \
     reported as SOLID (default: 20, range: 1-100000)\n  \
     -h, --help         Print this help message"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn solid_depth_defaults_without_arguments() {
        assert_eq!(parse_solid_depth(std::iter::empty::<&str>()), Ok(DEFAULT_SOLID_DEPTH));
    }

    #[test]
    fn solid_depth_ignores_unrelated_arguments() {
        let args = ["--seed-nodes", "1.2.3.4:18888", "--verbose"];
        assert_eq!(parse_solid_depth(args), Ok(DEFAULT_SOLID_DEPTH));
    }

    #[test]
    fn solid_depth_accepts_separate_value() {
        assert_eq!(parse_solid_depth(["--solid-depth", "35"]), Ok(35));
    }

    #[test]
    fn solid_depth_accepts_inline_value() {
        assert_eq!(parse_solid_depth(["--solid-depth=45"]), Ok(45));
    }

    #[test]
    fn solid_depth_last_occurrence_wins() {
        let args = ["--solid-depth=30", "--solid-depth", "19"];
        assert_eq!(parse_solid_depth(args), Ok(19));
    }

    #[test]
    fn solid_depth_accepts_lower_bound() {
        assert_eq!(parse_solid_depth(["--solid-depth", "1"]), Ok(MIN_SOLID_DEPTH));
    }

    #[test]
    fn solid_depth_accepts_upper_bound() {
        assert_eq!(
            parse_solid_depth(["--solid-depth", "100000"]),
            Ok(MAX_SOLID_DEPTH)
        );
    }

    #[test]
    fn solid_depth_rejects_zero() {
        let err = parse_solid_depth(["--solid-depth", "0"]).unwrap_err();
        assert!(err.contains("between 1 and 100000"), "unexpected error: {err}");
    }

    #[test]
    fn solid_depth_rejects_above_maximum() {
        let err = parse_solid_depth(["--solid-depth", "100001"]).unwrap_err();
        assert!(err.contains("between 1 and 100000"), "unexpected error: {err}");
    }

    #[test]
    fn solid_depth_rejects_non_numeric_value() {
        let err = parse_solid_depth(["--solid-depth", "twenty"]).unwrap_err();
        assert!(err.contains("expected a positive integer"), "unexpected error: {err}");
    }

    #[test]
    fn solid_depth_rejects_negative_value() {
        let err = parse_solid_depth(["--solid-depth", "-5"]).unwrap_err();
        assert!(err.contains("expected a positive integer"), "unexpected error: {err}");
    }

    #[test]
    fn solid_depth_rejects_missing_value() {
        let err = parse_solid_depth(["--solid-depth"]).unwrap_err();
        assert!(err.contains("requires a value"), "unexpected error: {err}");
    }

    #[test]
    fn solid_depth_rejects_empty_inline_value() {
        let err = parse_solid_depth(["--solid-depth="]).unwrap_err();
        assert!(err.contains("expected a positive integer"), "unexpected error: {err}");
    }
}
