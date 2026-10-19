use crate::state::NodeState;
use std::sync::Arc;
use tonic::{Request, Response, Status, Streaming};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

pub mod api_proto {
    tonic::include_proto!("api");
}

use api_proto::node_api_server::{NodeApi, NodeApiServer};
use api_proto::{SubscriptionAction, TransactionEvent};

/// gRPC Service implementation for the Node API.
pub struct ApiService {
    /// Shared node state and subscriber registry.
    state: Arc<NodeState>,
}

impl ApiService {
    pub fn new(state: Arc<NodeState>) -> Self {
        Self { state }
    }
}

/// Check if a string looks like a hex Tron address (42 hex chars = 21 bytes with 0x41 prefix).
fn is_hex_addr(addr: &str) -> bool {
    addr.len() == 42 && addr.bytes().all(|b| b.is_ascii_hexdigit())
}

#[tonic::async_trait]
impl NodeApi for ApiService {
    type SubscribeStream = std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<TransactionEvent, Status>> + Send>>;

    /// Main entry point for the gRPC bidirectional stream.
    ///
    /// The client sends a stream of SubscriptionActions (ADD/REMOVE addresses).
    /// The server returns a stream of TransactionEvents in JSON format.
    async fn subscribe(
        &self,
        request: Request<Streaming<SubscriptionAction>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let mut stream = request.into_inner();
        // Use bounded channel to protect against OOM
        let (tx, rx) = mpsc::channel(1024);

        let session_id = self.state.next_session_id();
        let state = self.state.clone();

        // Spawn a handler to process incoming actions for this session
        tokio::spawn(async move {
            while let Some(result) = stream.next().await {
                match result {
                    Ok(action_req) => {
                        for addr in &action_req.addresses {
                            let addr_key = if is_hex_addr(addr) {
                                addr.to_lowercase()
                            } else {
                                addr.clone() 
                            };

                            match action_req.action() {
                                api_proto::subscription_action::Action::Add => {
                                    state.add_subscriber(session_id, addr_key.clone(), tx.clone());
                                    tracing::info!("[Session {}] Subscribed: '{}'", session_id, addr_key);
                                }
                                api_proto::subscription_action::Action::Remove => {
                                    state.remove_subscriber(session_id, &addr_key);
                                    tracing::info!("[Session {}] Unsubscribed: '{}'", session_id, addr_key);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("[Session {}] Incoming stream error: {}", session_id, e);
                        break;
                    }
                }
            }
            // Cleanup session when stream closes
            state.cleanup_session(session_id);
            tracing::info!("[Session {}] Connection closed, cleaned up subscriptions", session_id);
        });

        let output_stream = ReceiverStream::new(rx).map(|event| {
            tracing::debug!("Yielding event to gRPC stream: len={}", event.transaction_json.len());
            Ok(event)
        });
        Ok(Response::new(Box::pin(output_stream) as Self::SubscribeStream))
    }
}

/// Starts the gRPC API server on the specified address.
pub async fn start_api_server(state: Arc<NodeState>, addr_str: String) -> Result<(), Box<dyn std::error::Error>> {
    let addr = addr_str.parse()?;
    let service = ApiService::new(state);

    tracing::info!("gRPC API server listening on {}", addr);

    tonic::transport::Server::builder()
        .add_service(NodeApiServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
