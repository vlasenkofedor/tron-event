//! TRON Event Node — Rust gRPC Subscriber Example
//!
//! This example demonstrates how to connect to a running tron-event node
//! via gRPC, subscribe to address filters, and stream transaction events.
//!
//! Usage:
//!   cargo run --release -- <address1> [address2] ...
//!
//! Example:
//!   cargo run --release -- TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t

use tokio_stream::StreamExt;
use tonic::Request;

// Generated protobuf types from node_api.proto
pub mod api_proto {
    tonic::include_proto!("api");
}

use api_proto::node_api_client::NodeApiClient;
use api_proto::subscription_action::Action;
use api_proto::SubscriptionAction;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ── Parse command-line arguments ────────────────────────────────────────
    let args: Vec<String> = std::env::args().collect();
    let addresses: Vec<String> = if args.len() > 1 {
        args[1..].to_vec()
    } else {
        // Default: subscribe to the USDT contract address
        vec!["TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t".to_string()]
    };

    println!("TRON Event Node — Rust Subscriber");
    println!("==================================");
    println!("Monitoring {} address(es):", addresses.len());
    for addr in &addresses {
        println!("  - {}", addr);
    }
    println!();

    // ── Connect to the gRPC server ──────────────────────────────────────────
    // The node listens on 0.0.0.0:50051 by default
    let server_addr = "http://127.0.0.1:50051";
    println!("Connecting to {}...", server_addr);

    let mut client = NodeApiClient::connect(server_addr).await?;
    println!("Connected successfully!\n");

    // ── Create the bidirectional stream ─────────────────────────────────────
    // The Subscribe RPC is a bidirectional stream:
    //   - Client sends SubscriptionAction messages (ADD/REMOVE addresses)
    //   - Server streams TransactionEvent messages (JSON payloads)
    let (tx, rx) = tokio::sync::mpsc::channel(100);
    let request_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

    println!("Opening subscription stream...");
    let mut stream = client
        .subscribe(Request::new(request_stream))
        .await?
        .into_inner();

    // ── Send initial ADD subscriptions ─────────────────────────────────────
    // We send one SubscriptionAction per address with Action::ADD
    for addr in &addresses {
        let action = SubscriptionAction {
            action: Action::Add as i32,
            addresses: vec![addr.clone()],
        };
        tx.send(action).await?;
        println!("  [SENT] ADD: {}", addr);
    }
    println!();

    // ── Stream and process transaction events ──────────────────────────────
    // Each event contains a JSON payload with:
    //   - txid: transaction hash
    //   - type: transaction type (TRX_TRANSFER, TRC20_TRANSFER, etc.)
    //   - status: PENDING_MEMPOOL, MINED, or SOLID
    //   - update_type: NEW_TRANSACTION or STATUS_CHANGE
    //   - details: address-specific data (from, to, amount, symbol, etc.)
    println!("Listening for transaction events...\n");

    let mut event_count = 0u64;
    let start_time = std::time::Instant::now();

    while let Some(result) = stream.next().await {
        match result {
            Ok(event) => {
                event_count += 1;

                // Parse the JSON payload from the event
                let json: serde_json::Value = serde_json::from_str(&event.transaction_json)
                    .unwrap_or_else(|_| serde_json::json!({ "error": "invalid JSON" }));

                // Extract key fields for display
                let txid = json.get("txid").and_then(|v| v.as_str()).unwrap_or("unknown");
                let tx_type = json.get("type").and_then(|v| v.as_str()).unwrap_or("unknown");
                let status = json.get("status").and_then(|v| v.as_str()).unwrap_or("unknown");
                let update_type = json.get("update_type").and_then(|v| v.as_str()).unwrap_or("unknown");

                // Extract details
                let details = json.get("details").cloned().unwrap_or(serde_json::json!({}));
                let from = details.get("from").and_then(|v| v.as_str()).unwrap_or("-");
                let to = details.get("to").and_then(|v| v.as_str()).unwrap_or("-");
                let amount = details.get("amount").and_then(|v| v.as_str()).unwrap_or("-");
                let symbol = details.get("symbol").and_then(|v| v.as_str()).unwrap_or("-");

                println!(
                    "[EVENT #{}] {} | {} | {} | {}",
                    event_count, txid, tx_type, status, update_type
                );
                println!(
                    "  from={}, to={}, amount={}, symbol={}",
                    from, to, amount, symbol
                );

                // Print elapsed time every 10 events
                if event_count % 10 == 0 {
                    let elapsed = start_time.elapsed().as_secs_f64();
                    println!(
                        "\n  [STATS] {} events in {:.1}s ({:.1} events/s)\n",
                        event_count,
                        elapsed,
                        event_count as f64 / elapsed
                    );
                }
            }
            Err(status) => {
                eprintln!("gRPC stream error: {}", status);
                break;
            }
        }
    }

    // ── Summary ─────────────────────────────────────────────────────────────
    let elapsed = start_time.elapsed().as_secs_f64();
    println!("\n==================================");
    println!("Stream closed.");
    println!("Total events: {}", event_count);
    println!("Elapsed time: {:.1}s", elapsed);
    if elapsed > 0.0 {
        println!("Average rate: {:.1} events/s", event_count as f64 / elapsed);
    }

    Ok(())
}
