use tracing::warn;



pub fn ip_to_bytes(ip: &str) -> Vec<u8> {
    // TRON P2P strictly expects IP as an ASCII STRING, not raw 4 bytes.
    ip.as_bytes().to_vec()
}

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

pub fn compute_block_id(number: i64, raw: &crate::protocol::protocol::block_header::Raw) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    use prost::Message;
    let mut buf = Vec::new();
    raw.encode(&mut buf).unwrap();
    let mut hasher = Sha256::new();
    hasher.update(&buf);
    let mut hash = hasher.finalize().to_vec();
    // Overwrite first 8 bytes with block number
    let num_bytes = number.to_be_bytes();
    hash[0..8].copy_from_slice(&num_bytes);
    hash
}

pub fn protocol_transaction_id(tx: &crate::protocol::protocol::Transaction) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    use prost::Message;
    if let Some(raw) = &tx.raw_data {
        let mut buf = Vec::new();
        raw.encode(&mut buf).unwrap();
        let mut hasher = Sha256::new();
        hasher.update(&buf);
        hasher.finalize().to_vec()
    } else {
        vec![0u8; 32]
    }
}
