use api::node_api_client::NodeApiClient;
use api::SubscriptionAction;
use api::subscription_action::Action;
use tokio_stream::StreamExt;

pub mod api {
    tonic::include_proto!("api");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = NodeApiClient::connect("http://0.0.0.0:50051").await?;
    println!("Connected to Tron Event Node gRPC at :50051");

    // USDT Contract Address as requested by user
    let usdt_addr = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t";

    // Create a stream to send actions to the server
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    
    // Initial action: Subscribe to USDT
    tx.send(SubscriptionAction {
        action: Action::Add as i32,
        addresses: vec![usdt_addr.to_string()],
    }).await?;

    println!("Subscribed to USDT: {}", usdt_addr);
    println!("Waiting for transaction events...");

    let mut stream = client.subscribe(tokio_stream::wrappers::ReceiverStream::new(rx)).await?.into_inner();

    while let Some(event) = stream.next().await {
        match event {
            Ok(e) => {
                println!("--- TRANSACTION RECEIVED ---");
                println!("{}", e.transaction_json);
            }
            Err(status) => {
                eprintln!("gRPC Stream Error: {}", status);
                break;
            }
        }
    }

    Ok(())
}
