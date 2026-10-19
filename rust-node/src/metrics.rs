//! Prometheus-compatible metrics for the Tron event node.
//!
//! Exposes a `/metrics` HTTP endpoint with gauges and counters for
//! chain status, peer connections, transaction processing, and gRPC activity.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::state::NodeState;

/// Snapshot of all metrics at a point in time.
#[derive(Debug, Clone, Default)]
pub struct MetricsSnapshot {
    /// Current chain head block number.
    pub chain_head: u64,
    /// Block head advertised by peers (gossip).
    pub gossip_head: u64,
    /// Number of blocks pending to fill gaps.
    pub pending_gaps: usize,
    /// Active peer connections.
    pub active_peers: usize,
    /// Transactions currently being tracked.
    pub tracked_txs: usize,
    /// Total inventory-type messages received.
    pub inventory_tx_total: u64,
    /// Total blocks processed.
    pub blocks_total: u64,
    /// Total USDT transfers detected.
    pub usdt_total: u64,
}

impl MetricsSnapshot {
    /// Collect a fresh snapshot from the shared node state.
    pub fn collect(state: &NodeState) -> Self {
        let (chain_head, _) = state.last_block();
        Self {
            chain_head,
            gossip_head: state.head_gossip_num.load(Ordering::Relaxed),
            pending_gaps: state.pending_gaps.lock().len(),
            active_peers: state.active_peers.len(),
            tracked_txs: state.tracked_txs.len(),
            inventory_tx_total: state.stats_inventory_tx.load(Ordering::Relaxed),
            blocks_total: state.stats_blocks.load(Ordering::Relaxed),
            usdt_total: state.stats_usdt.load(Ordering::Relaxed),
        }
    }

    /// Render metrics in Prometheus text exposition format.
    pub fn render(&self) -> String {
        let mut output = String::new();
        output.push_str(&format!("tron_chain_head {}\n", self.chain_head));
        output.push_str(&format!("tron_gossip_head {}\n", self.gossip_head));
        output.push_str(&format!("tron_pending_gaps {}\n", self.pending_gaps));
        output.push_str(&format!("tron_active_peers {}\n", self.active_peers));
        output.push_str(&format!("tron_tracked_txs {}\n", self.tracked_txs));
        output.push_str(&format!("tron_inventory_tx_total {}\n", self.inventory_tx_total));
        output.push_str(&format!("tron_blocks_total {}\n", self.blocks_total));
        output.push_str(&format!("tron_usdt_total {}\n", self.usdt_total));
        output
    }
}

/// Metrics collector that periodically snapshots node state.
pub struct MetricsCollector;

impl MetricsCollector {
    /// Create a new metrics collector.
    pub fn new() -> Self {
        Self
    }

    /// Collect a snapshot from the given state.
    pub fn snapshot(&self, state: &Arc<NodeState>) -> MetricsSnapshot {
        MetricsSnapshot::collect(state)
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_snapshot_render() {
        let snapshot = MetricsSnapshot {
            chain_head: 81728607,
            gossip_head: 87246480,
            pending_gaps: 5,
            active_peers: 3,
            tracked_txs: 42,
            inventory_tx_total: 1000,
            blocks_total: 500,
            usdt_total: 250,
        };
        let rendered = snapshot.render();
        assert!(rendered.contains("tron_chain_head 81728607"));
        assert!(rendered.contains("tron_active_peers 3"));
        assert!(rendered.contains("tron_usdt_total 250"));
    }
}
