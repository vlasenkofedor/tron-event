# TRON Event Node — Examples

This directory contains client examples for consuming the gRPC event stream from a running tron-event node.

## Prerequisites

- A running tron-event node (see [README](../README.md) for build & run instructions)
- For Rust example: `cargo` (Rust 1.75+)
- For Node.js example: Node.js 18+ and `npm`

## Available Examples

| Example | Language | Description |
|---------|----------|-------------|
| [`rust/`](rust/) | Rust | Full-featured subscriber with JSON parsing and stats |
| [`js/`](js/) | Node.js | Streaming subscriber with event logging and SIGINT handling |

## Rust Example

### Build & Run

```bash
cd rust
cargo build --release
cargo run --release -- <TRON_ADDRESS>

# Example: monitor USDT contract
cargo run --release -- TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t

# Example: monitor multiple addresses
cargo run --release -- TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t TQrY8tryqsYVCYS3MFbtffiPp2ccyn4STm
```

### Features
- Connects to gRPC server at `127.0.0.1:50051`
- Subscribes to one or more TRON addresses
- Parses JSON event payloads and displays structured output
- Shows real-time stats (events/second) every 10 events

---

## Node.js Example

### Install & Run

```bash
cd js
npm install
node index.js <TRON_ADDRESS>

# Example: monitor USDT contract
node index.js TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t

# Example: monitor multiple addresses
node index.js TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t TQrY8tryqsYVCYS3MFbtffiPp2ccyn4STm
```

### Features
- Connects to gRPC server at `127.0.0.1:50051`
- Subscribes to one or more TRON addresses
- Parses JSON event payloads and displays structured output
- Shows real-time stats (events/second) every 10 events
- Graceful shutdown on `Ctrl+C` (SIGINT)

---

## Load Testing

The Rust example crate also builds a benchmark tool that measures subscription
capacity, registration throughput and event delivery against a running node.

```bash
cd rust
cargo build --release

# 500,000 synthetic addresses, 20-second measurement window
./target/release/load_test http://127.0.0.1:50051 500000 20
```

Arguments:

| Argument | Default | Meaning |
| :--- | :--- | :--- |
| `node` | `http://127.0.0.1:50051` | gRPC endpoint of the node |
| `address count` | `10000` | Number of synthetic addresses to register |
| `window seconds` | `20` | How long to measure delivery and sample node memory |

### Measuring real event delivery

Synthetic addresses never appear in real transactions, so they only exercise
subscription bookkeeping. To measure actual event throughput, feed addresses
that are active on chain:

```bash
LOAD_TEST_REAL_ADDRS=1 \
REAL_ADDRS="TAddr1,TAddr2,TAddr3" \
./target/release/load_test http://127.0.0.1:50051 3 90
```

With `LOAD_TEST_REAL_ADDRS=1` the address-count argument is ignored and the
comma-separated list from `REAL_ADDRS` is used instead. A window of 90 seconds
or more is recommended, since popular addresses are not guaranteed to transact
on every block.

The tool prints registration throughput, events received during the window,
events per second, and peak/steady memory usage of the node process.

A 90-second real-address run with the 300 most active Mainnet addresses
delivered 3,598 events (≈40 events/s, ~1.1 MB of JSON) while the node used
69 MB RSS and 2.3 % CPU. Since every transfer emits `PENDING_MEMPOOL`,
`MINED` and `SOLID` updates, that window covered roughly 1,200 transactions.

### Reference results

Measured on an AMD Ryzen 9 9950X / 125 GiB RAM machine, `--release` build, node
synced to Tron Mainnet:

| Addresses | Node RSS | Node CPU | Registration throughput |
| ---: | ---: | ---: | ---: |
| 10,000 | 58 MB | 0.3 % | 17.4 M addr/s |
| 100,000 | 78 MB | 0.4 % | 23.9 M addr/s |
| 500,000 | 190 MB | 0.6 % | 28.7 M addr/s |
| 2,000,000 | 588 MB | 1.5 % | 1.4 M addr/s |

Roughly 270–300 bytes of RAM are needed per tracked address. A 512 MB machine
comfortably handles about 500,000 subscriptions; see the
[main README](../README.md#capacity--resource-usage) for sizing guidance.

---

## Event Schema

Each event received from the stream contains a JSON payload:

```json
{
  "txid": "b3f09a8ec8930419c8491040319ca0194...",
  "type": "TRC20_TRANSFER",
  "status": "PENDING_MEMPOOL",
  "update_type": "NEW_TRANSACTION",
  "details": {
    "from": "TQrY8tryqsYVCYS3MFbtffiPp2ccyn4STm",
    "to": "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
    "amount": "100000000",
    "symbol": "USDT",
    "timestamp": 1713000000000
  }
}
```

### Status Lifecycle

Every transaction progresses through three states:

1. **PENDING_MEMPOOL** — seen in the network mempool (before block inclusion)
2. **MINED** — included in a mined block
3. **SOLID** — confirmed 19+ blocks deep (irreversible)

When a transaction reaches `SOLID`, a `STATUS_CHANGE` update is emitted.
