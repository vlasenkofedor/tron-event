# Tron Node API Documentation

This document describes the gRPC API and JSON event structures for the Tron Transaction Monitoring Node.

## Overview

The node monitors the Tron P2P network in real-time, extracts TRC20 transfers and other contracts, and pushes them to subscribers via a gRPC bidirectional stream.

## gRPC Service

**Service**: `api.NodeApi`  
**Method**: `Subscribe` (Bidirectional Stream)

### Request (SubscriptionAction)
```protobuf
message SubscriptionAction {
  enum Action {
    ADD = 0;
    REMOVE = 1;
  }
  Action action = 1;
  repeated string addresses = 2; // Hex or Base58 (e.g. TR7NH...)
}
```

### Response (TransactionEvent)
```protobuf
message TransactionEvent {
  string transaction_json = 1;
}
```

## JSON Event Schema

Every event is a JSON object. There are two main types of updates defined by `update_type`.

### 1. New Transaction (`NEW_TRANSACTION`)
Sent immediately when a transaction is detected in a block (`MINED`) or in the mempool (`PENDING_MEMPOOL`).

**Fields**:
- `txid`: Full transaction hash (hex).
- `type`: Contract type (e.g., `TRC20_TRANSFER`, `TRX_TRANSFER`).
- `status`: Current status (`MINED` or `PENDING_MEMPOOL`).
- `details`:
  - `from`: Sender address (Base58).
  - `to`: Recipient address (Base58).
  - `amount`: Value in SUN or tokens (string).
  - `symbol`: e.g., `USDT` or `TRC20`.
  - `timestamp`: Creation time (Unix ms).

### 2. Status Change (`STATUS_CHANGE`)
Sent when a previously detected transaction reaches a new lifecycle stage.

**Fields**:
- `txid`: Transaction hash.
- `status`: New status (`SOLID`).
- `update_type`: Always `STATUS_CHANGE`.
- `details`: Full transaction metadata (same as `NEW_TRANSACTION`).

> [!IMPORTANT]
> **Solid Confirmation**: In Tron, a transaction is considered `SOLID` (irreversible) after 19 blocks on the canonical chain. This node uses **Fork Detection** to ensure the status is only emitted for a block on the main branch. The confirmation depth is configurable with the `--solid-depth <N>` command-line option and defaults to **20** (19 blocks of protocol finality plus a one-block safety margin). The configured value is printed in the startup log.

## Lifecycle States

| Status | Description |
| :--- | :--- |
| `PENDING_MEMPOOL` | Transaction seen in the gossip network but not yet in a block. |
| `MINED` | Transaction successfully included in a block observed over P2P. |
| `SOLID` | Transaction reached finality: its block is at least `--solid-depth` below the local head (default `20`). This is the **terminal state**. |

Both `MINED` and `SOLID` are derived from the node's own P2P-synced chain, without any upstream RPC endpoint. `PENDING_MEMPOOL → MINED` happens when the containing block is applied; `MINED → SOLID` is emitted by the status worker once the local head is deep enough. A transaction that never appears in an observed block stays `PENDING_MEMPOOL` and is dropped by the hourly cleanup after one hour.

## Contract Events

Transfers are not the only events. The parser also decodes resource and staking operations; they use the same envelope (`txid`, `type`, `status`, `update_type`, `details`) and the same lifecycle.

| `type` | Tron contract (number) | `details` fields |
| :--- | :--- | :--- |
| `TRX_TRANSFER` | `TransferContract` (1) | `from`, `to`, `amount` |
| `FREEZE_V1` | `FreezeBalanceContract` (11) | `owner`, `receiver` |
| `UNFREEZE_V1` | `UnfreezeBalanceContract` (12) | `owner` |
| `TRC20_TRANSFER` | `TriggerSmartContract` (31), method `transfer(address,uint256)` | `from`, `to`, `contract_address`, `amount`, `symbol` |
| `FREEZE_V2` | `FreezeBalanceV2Contract` (54) | `owner` |
| `UNFREEZE_V2` | `UnfreezeBalanceV2Contract` (55) | `owner` |
| `WITHDRAW_EXPIRE_UNFREEZE` | `WithdrawExpireUnfreezeContract` (56) | `owner` |
| `DELEGATE_RESOURCE` | `DelegateResourceContract` (57) | `owner`, `receiver`, `resource`, `amount`, `lock`, `lock_period` |
| `UNDELEGATE_RESOURCE` | `UnDelegateResourceContract` (58) | `owner`, `receiver`, `resource`, `amount` |
| `CANCEL_ALL_UNFREEZE_V2` | `CancelAllUnfreezeV2Contract` (59) | `owner` |

Additional rules:

- `amount` is in **SUN** (1 TRX = 1,000,000 SUN).
- `resource` is `BANDWIDTH`, `ENERGY` or `TRON_POWER`.
- `DELEGATE_RESOURCE` and `UNDELEGATE_RESOURCE` are delivered to **both** the owner and the receiver. `FREEZE_V1` is delivered to both as well, because it can freeze resources for another account.
- The other staking operations have no counterparty and are delivered only to the owner.
- `DELEGATE_RESOURCE` always contains `lock` and `lock_period`. `lock = true` marks a Stake 2.0 locked delegation that cannot be undelegated until `lock_period` days have passed; a normal delegation reports `false` and `0`.
- A transaction that mentions the same address in several roles (for example a self-transfer or a self-delegation) delivers exactly one event per subscription.

`DELEGATE_RESOURCE` example:

```json
{
  "txid": "5f0c...",
  "type": "DELEGATE_RESOURCE",
  "status": "PENDING_MEMPOOL",
  "update_type": "NEW_TRANSACTION",
  "details": {
    "owner": "TQrY8tryqsYVCYS3MFbtffiPp2ccyn4STm",
    "receiver": "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
    "resource": "ENERGY",
    "amount": 15000000,
    "lock": true,
    "lock_period": 7,
    "timestamp": 1713000000000
  }
}
```

## Fork Detection & Finality

The node maintains a history of block hashes to protect against chain reorganizations. If a block is orphaned before reaching `SOLID` status, the node detects the fork and rolls back its internal state. For more technical details, see [Fork Detection Documentation](fork_detection.md).

## Support for Lite Nodes

This node is optimized for **Lite FullNodes**. It does not require transaction indexing or detailed receipts to monitor finality, making it high-performance and autonomous.
