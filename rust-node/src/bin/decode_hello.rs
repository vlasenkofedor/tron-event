use prost::Message;

pub mod protocol {
    include!(concat!(env!("OUT_DIR"), "/protocol.rs"));
}

fn main() {
    // 0x20 Payload (Tron Hello)

    // 1. Valid Java-Tron HEX generated via Python:
    let valid_hex = "0a510a093132372e302e302e3110c893011a400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000010e75618a8fdc3f7c73322220a200f5fcf0eaae85210c4d29f0ce64718cd39fe883a45c602c3752e557b4f3b79da2a220a200f5fcf0eaae85210c4d29f0ce64718cd39fe883a45c602c3752e557b4f3b79da32220a200f5fcf0eaae85210c4d29f0ce64718cd39fe883a45c602c3752e557b4f3b79da5a05342e382e31";

    // 2. Our Rust Node HEX dumped from logs right before "Connection error: IO Error: early eof":
    // Frame hex: d101200a510a093132372e302e302e3110c893011a40e469664c6087305092a6a0a3411a86b99e2f76a6c5aacf39fd781e961be3282b2f908f94f4bfbbfab9fb551174b924036185a094c89d63670edf0282e2e83ba710e75618b0e2e8f7c73322220a2000000000000000001ebf88508a03865c71d452e25f4d51194196a1d22b6653dc2a220a2000000000000000001ebf88508a03865c71d452e25f4d51194196a1d22b6653dc32220a2000000000000000001ebf88508a03865c71d452e25f4d51194196a1d22b6653dc5a05342e382e31
    // Let's extract the actual payload by stripping varint `d101` and type `20`:
    let rust_hex = "0a510a093132372e302e302e3110c893011a400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000010e75618dfe291f8c73322220a200f5fcf0eaae85210c4d29f0ce64718cd39fe883a45c602c3752e557b4f3b79da2a220a200f5fcf0eaae85210c4d29f0ce64718cd39fe883a45c602c3752e557b4f3b79da32220a200f5fcf0eaae85210c4d29f0ce64718cd39fe883a45c602c3752e557b4f3b79da5a05342e382e31";

    let valid_bytes = hex::decode(valid_hex).unwrap();
    let rust_bytes = hex::decode(rust_hex).unwrap();

    let valid_msg = protocol::HelloMessage::decode(&valid_bytes[..]).unwrap();
    let rust_msg = protocol::HelloMessage::decode(&rust_bytes[..]).unwrap();

    println!("==========================");
    println!("VALID JAVA-TRON HELLO:");
    println!("{:#?}", valid_msg);
    println!("--------------------------");
    println!("OUR RUST NODE HELLO:");
    println!("{:#?}", rust_msg);
    println!("==========================");

    if valid_bytes != rust_bytes {
        println!("The payloads DO NOT MATCH. Check the diff above.");
        let mut i = 0;
        let min_len = std::cmp::min(valid_bytes.len(), rust_bytes.len());
        while i < min_len {
            if valid_bytes[i] != rust_bytes[i] {
                println!(
                    "DIFFERENCE AT INDEX {}: Valid={:02x}, Rust={:02x}",
                    i, valid_bytes[i], rust_bytes[i]
                );
                println!(
                    "Valid bytes around index: {:02x?}",
                    &valid_bytes[i.saturating_sub(5)..std::cmp::min(valid_bytes.len(), i + 5)]
                );
                println!(
                    "Rust bytes around index:  {:02x?}",
                    &rust_bytes[i.saturating_sub(5)..std::cmp::min(rust_bytes.len(), i + 5)]
                );
                break;
            }
            i += 1;
        }
        println!(
            "Valid length: {}, Rust length: {}",
            valid_bytes.len(),
            rust_bytes.len()
        );
    } else {
        println!("The payloads MATCH perfectly!");
    }
}
