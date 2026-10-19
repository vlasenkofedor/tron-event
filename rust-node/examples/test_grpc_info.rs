use rust_node::protocol::protocol::wallet_client::WalletClient;
use rust_node::protocol::protocol::BytesMessage;
use tokio::time::{timeout, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let txid_hex = "a9b3803f139c7e57a70559f70aa8cf746af9e4faa7ef74662257be0f9cdcc582";
    let tx_hash = hex::decode(txid_hex)?;
    
    let nodes = vec![
        "tron.coinductor.io:50051",
        "grpc.trongrid.io:50051"
    ];

    for node in nodes {
        println!("\n=== Testing Node: {} ===", node);
        let mut client = match WalletClient::connect(format!("http://{}", node)).await {
            Ok(c) => c,
            Err(e) => {
                println!("Failed to connect: {}", e);
                continue;
            }
        };

        let req = BytesMessage { value: tx_hash.clone() };
        let result = timeout(Duration::from_secs(5), client.get_transaction_info_by_id(req)).await;

        match result {
            Ok(Ok(resp)) => {
                let info = resp.into_inner();
                println!("Block Number: {}", info.block_number);
                println!("Fee (SUN): {}", info.fee);
                if let Some(r) = info.receipt {
                    println!("Receipt found!");
                    println!("  Result: {:?}", r.result);
                    println!("  Net Usage: {}", r.net_usage);
                    println!("  Energy Usage: {}", r.energy_usage);
                    println!("  Energy Usage Total: {}", r.energy_usage_total);
                } else {
                    println!("Receipt: NONE (This node does not index transaction details)");
                }
            }
            Ok(Err(e)) => println!("gRPC Error: {}", e),
            Err(_) => println!("Timeout (5s)"),
        }
    }

    Ok(())
}
