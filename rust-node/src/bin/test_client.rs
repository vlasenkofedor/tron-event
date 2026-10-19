use tokio_stream::StreamExt;
use tokio::sync::mpsc;

pub mod api_proto {
    tonic::include_proto!("api");
}

use api_proto::node_api_client::NodeApiClient;
use api_proto::SubscriptionAction;
use api_proto::subscription_action::Action;

use std::io::Write;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let addr = args.get(1).map(|s| s.as_str()).unwrap_or("TQrY8tryqsYVCYS3MFbtffiPp2ccyn4STm");
    
    let mut client = NodeApiClient::connect("http://127.0.0.1:50051").await?;

    // Create a channel for outbound actions
    let (tx, rx) = mpsc::channel(10);
    let request_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

    // Start subscribe session
    print!("Connecting to node at http://127.0.0.1:50051... ");
    std::io::stdout().flush().unwrap();
    let mut stream = client.subscribe(request_stream).await?.into_inner();
    println!("OK");

    // Add address in real-time
    println!(">>> Sending ADD_ADDRESS for: {}", addr);
    tx.send(SubscriptionAction {
        action: Action::Add as i32,
        addresses: vec![addr.to_string()],
    }).await?;

    println!("Subscription active. Monitoring {}... (Ctrl+C to stop)", addr);
    std::io::stdout().flush().unwrap();

    while let Some(result) = stream.next().await {
        match result {
            Ok(event) => {
                println!("\n[EVENT RECEIVED] len={}\n{}", event.transaction_json.len(), event.transaction_json);
                std::io::stdout().flush().unwrap();
            }
            Err(e) => {
                eprintln!("Stream error: {}", e);
                break;
            }
        }
    }

    Ok(())
}
