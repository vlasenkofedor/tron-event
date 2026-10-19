use tracing::warn;

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ip_to_bytes() {
        assert_eq!(ip_to_bytes("127.0.0.1"), b"127.0.0.1".to_vec());
        assert_eq!(ip_to_bytes("192.168.1.1"), b"192.168.1.1".to_vec());
    }

    #[test]
    fn test_bytes_to_base58_empty() {
        assert_eq!(bytes_to_base58(&[]), "");
    }

    #[test]
    fn test_bytes_to_base58_known_address() {
        // 0x41 + 20 zero bytes -> known TRON address
        let mut addr = vec![0x41u8];
        addr.extend_from_slice(&[0u8; 20]);
        let b58 = bytes_to_base58(&addr);
        assert!(!b58.is_empty());
        // Should start with T (TRON mainnet prefix)
        assert!(b58.starts_with('T'));
    }

    #[test]
    fn test_protocol_transaction_id_empty() {
        let tx = crate::protocol::protocol::Transaction::default();
        let id = protocol_transaction_id(&tx);
        assert_eq!(id.len(), 32);
        assert!(id.iter().all(|&b| b == 0));
    }
}



/// Convert an IP address string to ASCII bytes for TRON P2P protocol.
///
/// TRON P2P expects IP addresses as ASCII strings (e.g., "127.0.0.1")
/// rather than raw 4-byte representation.
#[must_use]
pub fn ip_to_bytes(ip: &str) -> Vec<u8> {
    ip.as_bytes().to_vec()
}

/// Convert raw bytes to a TRON Base58Check address string.
///
/// Accepts 20-byte (no prefix) or 21-byte (with 0x41 prefix) addresses.
/// Returns a "T..." prefixed string for mainnet addresses.
#[must_use]
pub fn bytes_to_base58(addr: &[u8]) -> String {
    if addr.is_empty() { return String::new(); }
    
    // TRON addresses normally start with 0x41 (Mainnet). 
    let mut with_prefix = if addr.len() == 20 {
        let mut p = vec![0x41u8];
        p.extend_from_slice(addr);
        p
    } else if addr.len() == 21 {
        addr.to_vec()
    } else if addr.len() > 21 {
        // Already formatted? (21 bytes + 4 bytes checksum)
        if addr.len() == 25 {
            return bs58::encode(addr).into_string();
        }
        addr.to_vec()
    } else {
        addr.to_vec()
    };

    // Ensure it starts with 0x41 if it's 21 bytes
    if with_prefix.len() == 21 && with_prefix[0] != 0x41 {
        warn!("[UTILS] Address does not start with 0x41: {}", hex::encode(&with_prefix));
    }

    // Double SHA256 for checksum
    use sha2::{Digest, Sha256};
    let h1 = Sha256::digest(&with_prefix);
    let h2 = Sha256::digest(&h1);
    with_prefix.extend_from_slice(&h2[0..4]);

    bs58::encode(with_prefix).into_string()
}

/// Compute the block ID (hash) from raw block header data.
///
/// Encodes the protobuf header, SHA-256 hashes it, then overwrites
/// the first 8 bytes with the big-endian block number (TRON convention).
#[must_use]
pub fn compute_block_id(number: i64, raw: &crate::protocol::protocol::block_header::Raw) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    use prost::Message;
    let mut buf = Vec::new();
    raw.encode(&mut buf).expect("block header encoding should not fail");
    let mut hasher = Sha256::new();
    hasher.update(&buf);
    let mut hash = hasher.finalize().to_vec();
    // Overwrite first 8 bytes with block number
    let num_bytes = number.to_be_bytes();
    hash[0..8].copy_from_slice(&num_bytes);
    hash
}

/// Compute the transaction ID (hash) from a protocol Transaction.
///
/// SHA-256 of the encoded `raw_data`. Returns 32 zero bytes if no raw data.
#[must_use]
pub fn protocol_transaction_id(tx: &crate::protocol::protocol::Transaction) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    use prost::Message;
    if let Some(raw) = &tx.raw_data {
        let mut buf = Vec::new();
        raw.encode(&mut buf).expect("transaction encoding should not fail");
        let mut hasher = Sha256::new();
        hasher.update(&buf);
        hasher.finalize().to_vec()
    } else {
        vec![0u8; 32]
    }
}
