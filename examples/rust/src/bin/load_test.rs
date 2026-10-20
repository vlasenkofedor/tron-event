//! TRON Event Node — Load Test Tool
//!
//! Stress-tests the node's gRPC subscription API by registering a large number
//! of address filters and measuring:
//!   - gRPC stream establishment time
//!   - subscription registration throughput
//!   - node-side memory growth per subscribed address
//!   - event delivery rate under load
//!
//! Usage:
//!   cargo run --release --bin load_test -- [node_addr] [address_count]
//!
//! Set `LOAD_TEST_REAL_ADDRS=1` to read comma-separated real addresses from
//! the `REAL_ADDRS` env var instead of generating synthetic ones. This is
//! required to measure actual event delivery throughput.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tonic::Request;
use tokio::sync::mpsc;

// Generated protobuf types from node_api.proto
pub mod api_proto {
    tonic::include_proto!("api");
}

use api_proto::node_api_client::NodeApiClient;
use api_proto::subscription_action::Action;
use api_proto::SubscriptionAction;

/// Generate deterministic fake TRON addresses for load testing.
///
/// Uses a deterministic PRNG so runs are reproducible. Addresses are valid
/// Base58Check TRON mainnet format (0x41 prefix + 20 bytes + 4-byte checksum).
fn generate_addresses(count: usize) -> Vec<String> {
    use sha2::{Digest, Sha256};
    let mut out = Vec::with_capacity(count);
    let mut seed: u64 = 0x9E3779B97F4A7C15;

    for _ in 0..count {
        // xorshift64* PRNG for deterministic, cheap randomness
        seed ^= seed >> 12;
        seed ^= seed << 25;
        seed ^= seed >> 27;
        let r = seed.wrapping_mul(0x2545F4914F6CDD1D);

        // Build a 21-byte address: 0x41 prefix + 20 random bytes
        let mut raw = vec![0x41u8];
        raw.extend_from_slice(&r.to_be_bytes());
        raw.extend_from_slice(&((r >> 40) ^ 0xA5A5_5A5A_A5A5_5A5A).to_be_bytes());
        raw.extend_from_slice(&((r >> 8) ^ 0x5A5A_5A5A_5A5A_5A5A).to_be_bytes());

        // Base58Check: append 4-byte double-SHA256 checksum
        let h1 = Sha256::digest(&raw);
        let h2 = Sha256::digest(h1);
        raw.extend_from_slice(&h2[0..4]);

        out.push(bs58::encode(raw).into_string());
    }
    out
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let node_addr = args.get(1).cloned().unwrap_or_else(|| "http://127.0.0.1:50051".to_string());
    let address_count: usize = args
        .get(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);
    let window_secs: u64 = args
        .get(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);

    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║        TRON Event Node — Load Test                        ║");
    println!("╚══════════════════════════════════════════════════════════╝");
    println!();
    println!("  Target node:    {}", node_addr);
    println!("  Address count:  {}", address_count);
    println!();

    // ── Phase 1: Establish connection ──────────────────────────────────────
    println!("[PHASE 1] Establishing gRPC connection...");
    let conn_start = Instant::now();
    let mut client = NodeApiClient::connect(node_addr.clone()).await?;
    println!("  ✓ Connected in {:?}", conn_start.elapsed());
    println!();

    // ── Phase 2: Open subscription stream ──────────────────────────────────
    println!("[PHASE 2] Opening bidirectional subscription stream...");
    let (tx, rx) = mpsc::channel(1000);
    let request_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

    let stream_start = Instant::now();
    let mut stream = client
        .subscribe(Request::new(request_stream))
        .await?
        .into_inner();
    println!("  ✓ Stream open in {:?}", stream_start.elapsed());
    println!();

    // ── Phase 3: Register address filters in batches ───────────────────────
    println!("[PHASE 3] Registering {} address filters...", address_count);
    let addresses = if std::env::var("LOAD_TEST_REAL_ADDRS").is_ok() {
        // Use real addresses from the REAL_ADDRS env var (comma-separated).
        // Synthetic addresses never match real transactions, so event
        // throughput would always read as zero.
        let raw = std::env::var("REAL_ADDRS").unwrap_or_default();
        let list: Vec<String> = raw
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        println!("  (using {} real addresses from REAL_ADDRS)", list.len());
        list
    } else {
        generate_addresses(address_count)
    };
    let reg_start = Instant::now();

    const BATCH_SIZE: usize = 500;
    let mut sent = 0usize;

    for chunk in addresses.chunks(BATCH_SIZE) {
        let action = SubscriptionAction {
            action: Action::Add as i32,
            addresses: chunk.to_vec(),
        };
        tx.send(action).await?;
        sent += chunk.len();

        // Progress indicator every 5000 addresses
        if sent % 5000 == 0 {
            let elapsed = reg_start.elapsed();
            println!(
                "  ... {} / {} addresses ({:.0}/s)",
                sent,
                address_count,
                sent as f64 / elapsed.as_secs_f64()
            );
        }
    }

    let reg_elapsed = reg_start.elapsed();
    println!("  ✓ Registered {} addresses in {:?}", sent, reg_elapsed);
    println!(
        "  ✓ Registration throughput: {:.0} addresses/s",
        sent as f64 / reg_elapsed.as_secs_f64()
    );
    println!();

    // ── Phase 4: Measure event delivery over a window ─────────────────────
    println!("[PHASE 4] Measuring event delivery for {} seconds...", window_secs);
    let measure_window = Duration::from_secs(window_secs);
    let measure_start = Instant::now();

    // Spawn a task to drain the stream and count events
    let counter = std::sync::Arc::new(AtomicU64::new(0));
    let byte_counter = std::sync::Arc::new(AtomicU64::new(0));
    let c = counter.clone();
    let b = byte_counter.clone();

    let reader = tokio::spawn(async move {
        use tokio_stream::StreamExt;
        while let Some(Ok(event)) = stream.next().await {
            c.fetch_add(1, Ordering::Relaxed);
            b.fetch_add(event.transaction_json.len() as u64, Ordering::Relaxed);
        }
    });

    // Sample node memory during the measurement window
    let mut mem_samples = Vec::new();
    let mut cpu_samples = Vec::new();

    while measure_start.elapsed() < measure_window {
        tokio::time::sleep(Duration::from_secs(2)).await;
        if let Some(sample) = sample_process("rust-node") {
            mem_samples.push(sample.0);
            cpu_samples.push(sample.1);
        }
    }

    let event_count = counter.load(Ordering::Relaxed);
    let bytes_received = byte_counter.load(Ordering::Relaxed);
    reader.abort();

    let elapsed = measure_start.elapsed().as_secs_f64();
    let avg_mem = if mem_samples.is_empty() {
        0.0
    } else {
        mem_samples.iter().sum::<f64>() / mem_samples.len() as f64
    };
    let avg_cpu = if cpu_samples.is_empty() {
        0.0
    } else {
        cpu_samples.iter().sum::<f64>() / cpu_samples.len() as f64
    };

    println!("  ✓ Events received:        {}", event_count);
    println!("  ✓ Bytes received:         {} ({:.1} KB)", bytes_received, bytes_received as f64 / 1024.0);
    println!("  ✓ Event rate:             {:.1} events/s", event_count as f64 / elapsed);
    println!("  ✓ Avg node memory (RSS):  {:.1} MB", avg_mem);
    println!("  ✓ Avg node CPU:           {:.1} %", avg_cpu);
    println!();

    // ── Phase 5: Remove subscriptions (cleanup path) ──────────────────────
    println!("[PHASE 5] Testing REMOVE action (cleanup path)...");
    let rm_start = Instant::now();
    let action = SubscriptionAction {
        action: Action::Remove as i32,
        addresses: addresses.clone(),
    };
    tx.send(action).await?;
    let rm_elapsed = rm_start.elapsed();
    println!("  ✓ Sent REMOVE for {} addresses in {:?}", sent, rm_elapsed);
    println!();
    // ── Summary ─────────────────────────────────────────────────────────────
    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║                   Load Test Summary                       ║");
    println!("╠══════════════════════════════════════════════════════════╣");
    println!("║  Addresses subscribed:  {:>7}                          ║", sent);
    println!("║  Subscription time:     {:>7.3}s                         ║", reg_elapsed.as_secs_f64());
    println!("║  Sub throughput:        {:>7.0} addr/s                   ║", sent as f64 / reg_elapsed.as_secs_f64());
    println!("║  Events in window:      {:>7}                           ║", event_count);
    println!("║  Event rate:            {:>7.1} events/s                ║", event_count as f64 / elapsed);
    println!("║  Node RSS (avg):        {:>7.1} MB                       ║", avg_mem);
    println!("║  Node CPU (avg):        {:>7.1} %                        ║", avg_cpu);
    println!("╚══════════════════════════════════════════════════════════╝");

    Ok(())
}

/// Sample RSS memory and CPU usage of a process by name.
///
/// Returns (rss_mb, cpu_percent) or None if the process is not found.
fn sample_process(name: &str) -> Option<(f64, f64)> {
    let output = std::process::Command::new("ps")
        .args(["-eo", "rss,pcpu,comm"])
        .output()
        .ok()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines().skip(1) {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 3 && parts[2] == name {
            let rss_kb: f64 = parts[0].parse().ok()?;
            let cpu: f64 = parts[1].parse().ok()?;
            return Some((rss_kb / 1024.0, cpu));
        }
    }
    None
}
