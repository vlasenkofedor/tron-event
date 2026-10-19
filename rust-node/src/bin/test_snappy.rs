fn main() {
    let hex_payload = "12de01200a540a0c39312e3233392e35332e363210c893011a40dbc561a8cb36ee71a465cfa670a2b76501436e5ad9021ad0545083ccb1d649b9e7e0f48783b363d8565ddb215b31408dce749e3ff7bf9fd446bdbba0162c0e3d10e75618b0b8aedad73322220a2000000000000000001ebf88508a03865c71d452e25f4d51194196a1d22b6653dc2a270a200000000004df43fbf6c422105fb5a883dbec436df902e260e80234b09cb275c010fb87fd2632270a200000000004df440d5322a0b8e9afc9b80d9c6a3473111aef97f6652db3b749e8108d88fd265a05342e382e31";
    let body = hex::decode(hex_payload).unwrap();
    
    for len in 100..400 {
        let mut fixed = Vec::new();
        let mut val = len;
        loop {
            let mut byte = (val & 0x7F) as u8;
            val >>= 7;
            if val > 0 { byte |= 0x80; }
            fixed.push(byte);
            if val == 0 { break; }
        }
        fixed.extend_from_slice(&body);
        
        if let Ok(res) = snap::raw::Decoder::new().decompress_vec(&fixed) {
            println!("SUCCESS! Orig length varint was: {}, Decompressed len: {}, Hex: {}", len, res.len(), hex::encode(&res));
            return;
        }
    }
    println!("Failed to guess the varint length!");
}
