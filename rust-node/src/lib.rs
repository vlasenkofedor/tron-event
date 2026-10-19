//! Tron Event Node — high-performance P2P transaction monitor.
//!
//! This crate provides a lightweight TRON network sub-node that intercepts
//! transactions via the native P2P gossip protocol and streams events over gRPC.

pub mod protocol;
pub mod p2p;
pub mod state;
pub mod config;
pub mod error;
pub mod logging;
pub mod metrics;
pub mod net;
pub mod seed_nodes;
pub mod api;
