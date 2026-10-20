use crate::p2p::utils::{bytes_to_base58, protocol_transaction_id};
use crate::protocol;
use crate::state::{NodeState, TrackedTx, TransactionStatus};
use prost::Message;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::{Arc, Mutex, OnceLock};

/// Global file writer for transaction logs (avoids repeated OpenOptions).
static TX_LOG_WRITER: OnceLock<Arc<Mutex<std::fs::File>>> = OnceLock::new();

fn get_tx_log_writer() -> Arc<Mutex<std::fs::File>> {
    TX_LOG_WRITER.get_or_init(|| {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("transactions.log")
            .expect("Failed to open transactions.log");
        Arc::new(Mutex::new(file))
    }).clone()
}

/// Process a single transaction: parse contracts, broadcast events to subscribers.
///
/// Identifies transaction type (TRX transfer, TRC20, Freeze, Delegate, etc.),
/// extracts relevant fields, builds a JSON event, and notifies matching subscribers.
/// State label "confirmed" marks the tx as mined; anything else is mempool.
pub fn process_transaction(tx: &protocol::protocol::Transaction, state_label: &str, node_state: &NodeState, block_num: Option<u64>) {
    let tx_id_bytes = protocol_transaction_id(tx);
    let tx_id = hex::encode(&tx_id_bytes);

    if let Some(raw) = &tx.raw_data {
        let timestamp = raw.timestamp;
        tracing::debug!("[PARSER] Started processing tx: {} (label={})", tx_id, state_label);

        // Determine transaction state for tracking
        let is_mined = state_label == "confirmed";
        let current_status = if is_mined { TransactionStatus::Mined } else { TransactionStatus::Mempool };

        for contract in &raw.contract {
            let parameter = match contract.parameter.as_ref() {
                Some(p) => p,
                None => {
                    tracing::debug!("Transaction {} has no contract parameter", tx_id);
                    continue;
                }
            };

            // Route based on contract type
            match contract.r#type {
                1 => handle_trx_transfer(tx, &tx_id, parameter, timestamp, current_status, node_state, block_num),
                31 => handle_smart_contract(tx, &tx_id, parameter, timestamp, current_status, node_state, block_num),
                // Freeze/Unfreeze V1
                11 => handle_resource_v1(tx, &tx_id, parameter, timestamp, current_status, node_state, true, block_num),
                12 => handle_resource_v1(tx, &tx_id, parameter, timestamp, current_status, node_state, false, block_num),
                // Freeze/Unfreeze V2
                54 => handle_freeze_v2(tx, &tx_id, parameter, timestamp, current_status, node_state, block_num),
                55 => handle_unfreeze_v2(tx, &tx_id, parameter, timestamp, current_status, node_state, block_num),
                // Delegate
                57 => handle_delegate(tx, &tx_id, parameter, timestamp, current_status, node_state, block_num),
                58 => handle_undelegate(tx, &tx_id, parameter, timestamp, current_status, node_state, block_num),
                // Miscellaneous V2 Stake
                56 => handle_withdraw_expire_unfreeze(tx, &tx_id, parameter, timestamp, current_status, node_state, block_num),
                59 => handle_cancel_all_unfreeze_v2(tx, &tx_id, parameter, timestamp, current_status, node_state, block_num),
                _ => {
                    tracing::debug!("[PARSER] Skipping unsupported contract type={}", contract.r#type);
                }
            }
        }
    }
}

/// Specific handler for standard TRX (SUN) transfers.
fn handle_trx_transfer(
    _tx: &protocol::protocol::Transaction,
    tx_id: &str,
    parameter: &prost_types::Any,
    timestamp: i64,
    status: TransactionStatus,
    node_state: &NodeState,
    block_num: Option<u64>,
) {
    if let Ok(c) = protocol::protocol::TransferContract::decode(&parameter.value as &[u8]) {
        let from_hex = hex::encode(&c.owner_address);
        let to_hex = hex::encode(&c.to_address);
        let from_b58 = bytes_to_base58(&c.owner_address);
        let to_b58 = bytes_to_base58(&c.to_address);

        let details = serde_json::json!({
            "from": from_b58,
            "to": to_b58,
            "amount": c.amount
        });

        let event_json = build_event_json(tx_id, "TRX_TRANSFER", timestamp, status, Some(details));
        let enriched_details = event_json["details"].clone();
        broadcast_event(&[from_hex.as_str(), to_hex.as_str(), from_b58.as_str(), to_b58.as_str()], event_json, node_state, tx_id, status, block_num, "TRX_TRANSFER", enriched_details);
    }
}

/// Specific handler for TriggerSmartContract calls (TRC20 transfers, etc.).
fn handle_smart_contract(
    _tx: &protocol::protocol::Transaction,
    tx_id: &str,
    parameter: &prost_types::Any,
    timestamp: i64,
    status: TransactionStatus,
    node_state: &NodeState,
    block_num: Option<u64>,
) {
    if let Ok(c) = protocol::protocol::TriggerSmartContract::decode(&parameter.value as &[u8]) {
        if c.data.len() >= 68 && hex::encode(&c.data[0..4]) == "a9059cbb" {
            let contract_hex = hex::encode(&c.contract_address);
            let from_hex = hex::encode(&c.owner_address);
            let from_b58 = bytes_to_base58(&c.owner_address);
            let contract_b58 = bytes_to_base58(&c.contract_address);

            if contract_hex == "41a614f803b6fd780986a42c78ec9c7f77e6ded13c" {
                node_state.stats_usdt.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }

            let mut to_vec = vec![0x41u8];
            to_vec.extend_from_slice(&c.data[16..36]);
            let to_hex = hex::encode(&to_vec);
            let to_b58 = bytes_to_base58(&to_vec);

            let amount_raw = &c.data[36..68];
            let mut amount_val: u64 = 0;
            for i in 24..32 { amount_val = (amount_val << 8) | amount_raw[i] as u64; }

            let symbol = if contract_hex == "41a614f803b6fd780986a42c78ec9c7f77e6ded13c" { "USDT" } else { "TRC20" };

            let details = serde_json::json!({
                "from": from_b58,
                "to": to_b58,
                "contract_address": contract_b58,
                "amount": amount_val.to_string(),
                "symbol": symbol
            });

            let event_json = build_event_json(tx_id, "TRC20_TRANSFER", timestamp, status, Some(details));
            let enriched_details = event_json["details"].clone();
            broadcast_event(
                &[from_hex.as_str(), to_hex.as_str(), contract_hex.as_str(),
                  from_b58.as_str(), to_b58.as_str(), contract_b58.as_str()],
                event_json, node_state, tx_id, status, block_num, "TRC20_TRANSFER", enriched_details
            );
        }
    }
}

/// Specific handler for Resource V1 (Freeze/Unfreeze) operations.
fn handle_resource_v1(
    _tx: &protocol::protocol::Transaction,
    tx_id: &str,
    parameter: &prost_types::Any,
    timestamp: i64,
    status: TransactionStatus,
    node_state: &NodeState,
    is_freeze: bool,
    block_num: Option<u64>,
) {
    let tx_type = if is_freeze { "FREEZE_V1" } else { "UNFREEZE_V1" };
    if is_freeze {
        if let Ok(c) = protocol::protocol::FreezeBalanceContract::decode(&parameter.value as &[u8]) {
            let from_b58 = bytes_to_base58(&c.owner_address);
            let to_b58 = bytes_to_base58(&c.receiver_address);
            let details = serde_json::json!({ "owner": from_b58, "receiver": to_b58 });
            let event = build_event_json(tx_id, tx_type, timestamp, status, Some(details));
            let enriched_details = event["details"].clone();
            broadcast_event(
                &[hex::encode(&c.owner_address).as_str(), hex::encode(&c.receiver_address).as_str(), from_b58.as_str(), to_b58.as_str()],
                event, node_state, tx_id, status, block_num, tx_type, enriched_details
            );
        }
    } else {
        if let Ok(c) = protocol::protocol::UnfreezeBalanceContract::decode(&parameter.value as &[u8]) {
            let from_b58 = bytes_to_base58(&c.owner_address);
            let details = serde_json::json!({ "owner": from_b58 });
            let event = build_event_json(tx_id, tx_type, timestamp, status, Some(details));
            let enriched_details = event["details"].clone();
            broadcast_event(&[hex::encode(&c.owner_address).as_str(), from_b58.as_str()], event, node_state, tx_id, status, block_num, tx_type, enriched_details);
        }
    }
}

fn handle_freeze_v2(_tx: &protocol::protocol::Transaction, tx_id: &str, parameter: &prost_types::Any, timestamp: i64, status: TransactionStatus, node_state: &NodeState, block_num: Option<u64>) {
    if let Ok(c) = protocol::protocol::FreezeBalanceV2Contract::decode(&parameter.value as &[u8]) {
        let from_b58 = bytes_to_base58(&c.owner_address);
        let details = serde_json::json!({ "owner": from_b58 });
        let event = build_event_json(tx_id, "FREEZE_V2", timestamp, status, Some(details));
        let enriched_details = event["details"].clone();
        broadcast_event(&[hex::encode(&c.owner_address).as_str(), from_b58.as_str()], event, node_state, tx_id, status, block_num, "FREEZE_V2", enriched_details);
    }
}

fn handle_unfreeze_v2(_tx: &protocol::protocol::Transaction, tx_id: &str, parameter: &prost_types::Any, timestamp: i64, status: TransactionStatus, node_state: &NodeState, block_num: Option<u64>) {
    if let Ok(c) = protocol::protocol::UnfreezeBalanceV2Contract::decode(&parameter.value as &[u8]) {
        let from_b58 = bytes_to_base58(&c.owner_address);
        let details = serde_json::json!({ "owner": from_b58 });
        let event = build_event_json(tx_id, "UNFREEZE_V2", timestamp, status, Some(details));
        let enriched_details = event["details"].clone();
        broadcast_event(&[hex::encode(&c.owner_address).as_str(), from_b58.as_str()], event, node_state, tx_id, status, block_num, "UNFREEZE_V2", enriched_details);
    }
}

fn resource_code_to_str(code: i32) -> &'static str {
    match code {
        0 => "BANDWIDTH",
        1 => "ENERGY",
        2 => "TRON_POWER",
        _ => "UNKNOWN",
    }
}

fn handle_delegate(_tx: &protocol::protocol::Transaction, tx_id: &str, parameter: &prost_types::Any, timestamp: i64, status: TransactionStatus, node_state: &NodeState, block_num: Option<u64>) {
    if let Ok(c) = protocol::protocol::DelegateResourceContract::decode(&parameter.value as &[u8]) {
        let details = delegate_details(&c);
        let event = build_event_json(tx_id, "DELEGATE_RESOURCE", timestamp, status, Some(details));
        let enriched_details = event["details"].clone();
        broadcast_event(
            &[hex::encode(&c.owner_address).as_str(), hex::encode(&c.receiver_address).as_str(),
              bytes_to_base58(&c.owner_address).as_str(), bytes_to_base58(&c.receiver_address).as_str()],
            event, node_state, tx_id, status, block_num, "DELEGATE_RESOURCE", enriched_details
        );
    }
}

/// Build the `details` object for a resource delegation.
///
/// `lock` and `lock_period` describe Stake 2.0 locked delegations: the
/// delegated resources cannot be undelegated until the lock period (in days)
/// expires. Both fields are always present, so consumers can distinguish a
/// normal delegation (`lock: false`) from a locked one without guessing.
fn delegate_details(c: &protocol::protocol::DelegateResourceContract) -> serde_json::Value {
    let resource_str = resource_code_to_str(c.resource);
    serde_json::json!({
        "owner": bytes_to_base58(&c.owner_address),
        "receiver": bytes_to_base58(&c.receiver_address),
        "resource": resource_str,
        "amount": c.balance,
        "lock": c.lock,
        "lock_period": c.lock_period
    })
}

fn handle_undelegate(_tx: &protocol::protocol::Transaction, tx_id: &str, parameter: &prost_types::Any, timestamp: i64, status: TransactionStatus, node_state: &NodeState, block_num: Option<u64>) {
    if let Ok(c) = protocol::protocol::UnDelegateResourceContract::decode(&parameter.value as &[u8]) {
        let from_b58 = bytes_to_base58(&c.owner_address);
        let to_b58 = bytes_to_base58(&c.receiver_address);
        let resource_str = resource_code_to_str(c.resource);
        let details = serde_json::json!({ "owner": from_b58, "receiver": to_b58, "resource": resource_str, "amount": c.balance });
        let event = build_event_json(tx_id, "UNDELEGATE_RESOURCE", timestamp, status, Some(details));
        let enriched_details = event["details"].clone();
        broadcast_event(
            &[hex::encode(&c.owner_address).as_str(), hex::encode(&c.receiver_address).as_str(), from_b58.as_str(), to_b58.as_str()],
            event, node_state, tx_id, status, block_num, "UNDELEGATE_RESOURCE", enriched_details
        );
    }
}

fn handle_withdraw_expire_unfreeze(_tx: &protocol::protocol::Transaction, tx_id: &str, parameter: &prost_types::Any, timestamp: i64, status: TransactionStatus, node_state: &NodeState, block_num: Option<u64>) {
    if let Ok(c) = protocol::protocol::WithdrawExpireUnfreezeContract::decode(&parameter.value as &[u8]) {
        let from_b58 = bytes_to_base58(&c.owner_address);
        let details = serde_json::json!({ "owner": from_b58 });
        let event = build_event_json(tx_id, "WITHDRAW_EXPIRE_UNFREEZE", timestamp, status, Some(details));
        let enriched_details = event["details"].clone();
        broadcast_event(&[hex::encode(&c.owner_address).as_str(), from_b58.as_str()], event, node_state, tx_id, status, block_num, "WITHDRAW_EXPIRE_UNFREEZE", enriched_details);
    }
}

fn handle_cancel_all_unfreeze_v2(_tx: &protocol::protocol::Transaction, tx_id: &str, parameter: &prost_types::Any, timestamp: i64, status: TransactionStatus, node_state: &NodeState, block_num: Option<u64>) {
    if let Ok(c) = protocol::protocol::CancelAllUnfreezeV2Contract::decode(&parameter.value as &[u8]) {
        let from_b58 = bytes_to_base58(&c.owner_address);
        let details = serde_json::json!({ "owner": from_b58 });
        let event = build_event_json(tx_id, "CANCEL_ALL_UNFREEZE_V2", timestamp, status, Some(details));
        let enriched_details = event["details"].clone();
        broadcast_event(&[hex::encode(&c.owner_address).as_str(), from_b58.as_str()], event, node_state, tx_id, status, block_num, "CANCEL_ALL_UNFREEZE_V2", enriched_details);
    }
}

fn build_event_json(
    tx_id: &str,
    tx_type: &str,
    timestamp: i64,
    status: TransactionStatus,
    details: Option<serde_json::Value>
) -> serde_json::Value {
    let mut result = serde_json::json!({
        "txid": tx_id,
        "type": tx_type,
        "status": status.as_str(),
        "update_type": "NEW_TRANSACTION",
    });

    if let Some(mut d) = details {
        if let Some(obj) = d.as_object_mut() {
            obj.insert("timestamp".to_string(), serde_json::json!(timestamp));
        }
        result.as_object_mut().expect("json value is always an object").insert("details".to_string(), d);
    }

    result
}

fn broadcast_event(addrs: &[&str], json: serde_json::Value, node_state: &NodeState, tx_id: &str, status: TransactionStatus, block_num: Option<u64>, tx_type: &str, details: serde_json::Value) {
    let json_str = json.to_string();
    log_to_file(&json_str);
    
    let event = crate::api::api_proto::TransactionEvent {
        transaction_json: json_str,
    };

    let notified = node_state.notify_subscribers(addrs, event);
    
    if notified {
        let mut needs_insert = false;
        if let Some(mut tracked) = node_state.tracked_txs.get_mut(tx_id) {
            if tracked.status == TransactionStatus::Mempool && status == TransactionStatus::Mined {
                tracked.status = status;
                tracked.block_num = block_num;
                tracked.details = details.clone();
            } else if tracked.block_num.is_none() && block_num.is_some() {
                tracked.block_num = block_num;
                tracked.details = details.clone();
            }
        } else {
            needs_insert = true;
        }

        if needs_insert {
            node_state.tracked_txs.insert(tx_id.to_string(), TrackedTx {
                hash: hex::decode(tx_id).unwrap_or_default(),
                status,
                first_seen: std::time::Instant::now(),
                block_num,
                tx_type: tx_type.to_string(),
                notify_addresses: addrs.iter().map(|s| s.to_string()).collect(),
                details,
            });
        }
    }
}

fn log_to_file(line: &str) {
    let writer = get_tx_log_writer();
    if let Ok(mut file) = writer.lock() {
        let _ = writeln!(file, "{}", line);
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::protocol::DelegateResourceContract;

    fn addr(byte: u8) -> Vec<u8> {
        let mut addr = vec![0u8; 21];
        addr[0] = 0x41;
        addr[1] = byte;
        addr
    }

    #[test]
    fn delegate_details_include_lock_fields() {
        let owner = addr(1);
        let receiver = addr(2);

        let details = delegate_details(&DelegateResourceContract {
            owner_address: owner.clone(),
            resource: 1, // ENERGY
            balance: 15_000_000,
            receiver_address: receiver.clone(),
            lock: true,
            lock_period: 7,
        });

        assert_eq!(details["owner"], bytes_to_base58(&owner));
        assert_eq!(details["receiver"], bytes_to_base58(&receiver));
        assert_eq!(details["resource"], "ENERGY");
        assert_eq!(details["amount"], 15_000_000);
        assert_eq!(details["lock"], true);
        assert_eq!(details["lock_period"], 7);
    }

    #[test]
    fn delegate_details_represent_unlocked_delegation() {
        let details = delegate_details(&DelegateResourceContract {
            owner_address: addr(3),
            resource: 0, // BANDWIDTH
            balance: 1_000_000,
            receiver_address: addr(4),
            lock: false,
            lock_period: 0,
        });

        assert_eq!(details["resource"], "BANDWIDTH");
        assert_eq!(details["lock"], false);
        assert_eq!(details["lock_period"], 0);
    }

    #[test]
    fn delegate_details_map_tron_power_resource() {
        let details = delegate_details(&DelegateResourceContract {
            owner_address: addr(5),
            resource: 2, // TRON_POWER
            balance: 5_000_000,
            receiver_address: addr(6),
            lock: false,
            lock_period: 0,
        });

        assert_eq!(details["resource"], "TRON_POWER");
    }

    #[test]
    fn new_transaction_events_carry_update_type() {
        let event = build_event_json("abc", "TRX_TRANSFER", 1, TransactionStatus::Mempool, None);

        assert_eq!(event["update_type"], "NEW_TRANSACTION");
        assert_eq!(event["status"], "PENDING_MEMPOOL");
    }
}
