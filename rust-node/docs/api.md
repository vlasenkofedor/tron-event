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
> **Solid Confirmation**: In Tron, a transaction is considered `SOLID` (irreversible) after approximately 20 blocks on the canonical chain. This node uses **Fork Detection** to ensure this status is only emitted when the transaction is 20 blocks deep on the main branch.

## Lifecycle States

| Status | Description |
| :--- | :--- |
| `PENDING_MEMPOOL` | Transaction seen in the gossip network but not yet in a block. |
| `MINED` | Transaction successfully included in a block. |
| `SOLID` | Transaction reached finality (~20 blocks deep). This is the **terminal state**. |

## Fork Detection & Finality

The node maintains a history of block hashes to protect against chain reorganizations. If a block is orphaned before reaching `SOLID` status, the node detects the fork and rolls back its internal state. For more technical details, see [Fork Detection Documentation](fork_detection.md).

## Support for Lite Nodes

This node is optimized for **Lite FullNodes**. It does not require transaction indexing or detailed receipts to monitor finality, making it high-performance and autonomous.
