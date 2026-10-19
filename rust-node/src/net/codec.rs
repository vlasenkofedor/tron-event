use crate::net::P2pMessage;
use bytes::{BufMut, BytesMut};

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_varint_single_byte() {
        let data = [0x05];
        let (val, pos) = TronCodec::parse_varint(&data, 0);
        assert_eq!(val, 5);
        assert_eq!(pos, 1);
    }

    #[test]
    fn test_parse_varint_multi_byte() {
        // 300 = 0b10101100 0b00000010 -> varint: [0xAC, 0x02]
        let data = [0xAC, 0x02];
        let (val, pos) = TronCodec::parse_varint(&data, 0);
        assert_eq!(val, 300);
        assert_eq!(pos, 2);
    }

    #[test]
    fn test_write_varint_to_buf() {
        let mut buf = BytesMut::new();
        TronCodec::write_varint_to_buf(&mut buf, 300);
        assert_eq!(buf.to_vec(), vec![0xAC, 0x02]);
    }

    #[test]
    fn test_parse_endpoint_message_uncompressed() {
        // EndpointMessage with compress_type=0, data=[0x01, 0x02, 0x03]
        // Field 1 (compress_type): tag=0x08, value=0x00
        // Field 2 (data): tag=0x12, len=3, data=[0x01, 0x02, 0x03]
        let frame = [0x08, 0x00, 0x12, 0x03, 0x01, 0x02, 0x03];
        let result = TronCodec::parse_endpoint_message(&frame);
        assert!(result.is_ok());
        let (compress, data) = result.unwrap();
        assert_eq!(compress, 0);
        assert_eq!(data, vec![0x01, 0x02, 0x03]);
    }

    #[test]
    fn test_parse_endpoint_message_compressed() {
        // EndpointMessage with compress_type=1, data=[0xAA, 0xBB]
        let frame = [0x08, 0x01, 0x12, 0x02, 0xAA, 0xBB];
        let result = TronCodec::parse_endpoint_message(&frame);
        assert!(result.is_ok());
        let (compress, data) = result.unwrap();
        assert_eq!(compress, 1);
        assert_eq!(data, vec![0xAA, 0xBB]);
    }
}

/// TRON P2P wire protocol codec.
///
/// Handles Varint32 framing, EndpointMessage wrapping, and Snappy compression.
/// Message type IDs >= 0xfb are LibP2P layer; below that are Tron protocol layer.
pub struct TronCodec;

impl TronCodec {
    /// Read a single P2P message from the wire.
    ///
    /// Wire format (java-tron):
    ///   LibP2P messages (type_id >= 0xfb): [varint len][type_id][libp2p_protobuf]
    ///   Tron messages   (type_id <  0xfb): [varint len][EndpointMessage protobuf]
    ///
    /// EndpointMessage:
    ///   field 1 (varint):  compress_type  (0 = none, 1 = snappy)
    ///   field 2 (bytes):   data           = [type_id][protobuf_payload]
    pub async fn read_message<R>(
        reader: &mut R,
        _peer_compressed: bool,
        peer_addr: &str,
    ) -> crate::error::Result<P2pMessage>
    where
        R: tokio::io::AsyncReadExt + Unpin,
    {
        // 1. Read frame length
        let frame_len = Self::read_varint(reader).await?;
        if frame_len == 0 || frame_len > 10 * 1024 * 1024 {
            return Err(crate::error::NodeError::Network(
                format!("Invalid frame length: {}", frame_len),
            ));
        }

        // 2. Read frame bytes
        let mut frame = vec![0u8; frame_len as usize];
        reader.read_exact(&mut frame).await?;

        if frame.is_empty() {
            return Err(crate::error::NodeError::Network("Empty frame".to_string()));
        }

        // 3a. LibP2P messages: raw format [type_id][libp2p_proto]
        if frame[0] >= 0xfb {
            let type_id = frame[0];
            let payload = &frame[1..];
            tracing::info!(
                "[{}] IN: type_id=0x{:02x}, payload_len={}, format=libp2p_raw",
                peer_addr, type_id, payload.len()
            );
            return Ok(P2pMessage::decode(type_id, payload));
        }

        // 3b. Tron messages: EndpointMessage wrapper
        let (compress_type, inner_data) = Self::parse_endpoint_message(&frame)?;

        // 4. Decompress if needed
        let decoded = if compress_type == 1 {
            match snap::raw::Decoder::new().decompress_vec(&inner_data) {
                Ok(d) => d,
                Err(e) => {
                    tracing::warn!("[{}] Snappy decompress failed: {} — using raw", peer_addr, e);
                    inner_data
                }
            }
        } else {
            inner_data
        };

        if decoded.is_empty() {
            return Err(crate::error::NodeError::Network("Empty inner data".to_string()));
        }

        let type_id = decoded[0];
        let payload = &decoded[1..];

        tracing::info!(
            "[{}] IN: type_id=0x{:02x}, payload_len={}, compress={}",
            peer_addr, type_id, payload.len(), compress_type
        );

        Ok(P2pMessage::decode(type_id, payload))
    }

    /// Parse a TRON EndpointMessage protobuf into (compress_type, data).
    ///
    /// EndpointMessage wire format:
    ///   field 1 (varint):  compress_type  (0 = none, 1 = snappy)
    ///   field 2 (bytes):   data           = [type_id][protobuf_payload]
    fn parse_endpoint_message(buf: &[u8]) -> crate::error::Result<(u64, Vec<u8>)> {
        use prost::Message as _;

        #[derive(prost::Message)]
        struct EndpointMessage {
            #[prost(uint64, optional, tag = "1")]
            compress_type: Option<u64>,
            #[prost(bytes, optional, tag = "2")]
            data: Option<Vec<u8>>,
        }

        let msg = EndpointMessage::decode(buf)
            .map_err(|e| crate::error::NodeError::Network(format!("EndpointMessage decode failed: {}", e)))?;

        let compress_type = msg.compress_type.unwrap_or(0);
        let data = msg.data.ok_or_else(|| crate::error::NodeError::Network("EndpointMessage has no data".to_string()))?;

        Ok((compress_type, data))
    }

    /// Write a single P2P message to the wire.
    ///
    /// LibP2P messages (type_id >= 0xfb): [varint len][type_id][libp2p_proto]
    /// Tron messages   (type_id <  0xfb): [varint len][EndpointMessage{data=[type_id][proto]}]
    pub async fn write_message<W>(
        writer: &mut W,
        msg: &P2pMessage,
        compressed: bool,
        _peer_addr: &str,
    ) -> crate::error::Result<()>
    where
        W: tokio::io::AsyncWriteExt + Unpin,
    {
        let type_id = msg.get_type_id();

        // Encode protobuf payload
        let mut proto_buf = BytesMut::new();
        msg.encode(&mut proto_buf).map_err(|e| {
            crate::error::NodeError::Unknown(format!("Protobuf encode error: {}", e))
        })?;

        // Build frame
        let frame: Vec<u8> = if type_id >= 0xfb {
            // LibP2P: raw [type_id][proto]
            let mut f = vec![type_id];
            f.extend_from_slice(&proto_buf);
            f
        } else {
            // Tron: EndpointMessage wrapper
            // inner = [type_id][proto_bytes]
            let mut inner = vec![type_id];
            inner.extend_from_slice(&proto_buf);

            let (compress_field, payload) = if compressed {
                let comp = snap::raw::Encoder::new()
                    .compress_vec(&inner)
                    .map_err(|e| crate::error::NodeError::Unknown(format!("Snappy error: {}", e)))?;
                // compress_type field: tag=0x08 (field1,varint), value=0x01 (snappy)
                (vec![0x08u8, 0x01u8], comp)
            } else {
                (vec![], inner)
            };

            let mut env = BytesMut::new();
            env.put_slice(&compress_field);
            // data field: tag=0x12 (field2, len-delimited)
            env.put_u8(0x12);
            Self::write_varint_to_buf(&mut env, payload.len() as u64);
            env.put_slice(&payload);
            env.to_vec()
        };

        // Write [varint frame_len][frame]
        Self::write_varint(writer, frame.len() as u64).await?;
        writer.write_all(&frame).await?;
        writer.flush().await?;

        Ok(())
    }

    fn write_varint_to_buf(buf: &mut BytesMut, mut value: u64) {
        loop {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;
            if value > 0 { byte |= 0x80; }
            buf.put_u8(byte);
            if value == 0 { break; }
        }
    }

    async fn read_varint<R>(reader: &mut R) -> crate::error::Result<u64>
    where
        R: tokio::io::AsyncReadExt + Unpin,
    {
        let mut value: u64 = 0;
        let mut shift = 0u32;
        loop {
            let byte = reader.read_u8().await.map_err(|e| {
                crate::error::NodeError::Network(format!(
                    "Length read error (varint): {} — trying next node...", e
                ))
            })?;
            value |= ((byte & 0x7F) as u64) << shift;
            if (byte & 0x80) == 0 { break; }
            shift += 7;
            if shift > 63 {
                return Err(crate::error::NodeError::Network("Varint overflow".to_string()));
            }
        }
        Ok(value)
    }

    fn parse_varint(data: &[u8], mut pos: usize) -> (u64, usize) {
        let mut value: u64 = 0;
        let mut shift = 0u32;
        while pos < data.len() {
            let byte = data[pos]; pos += 1;
            value |= ((byte & 0x7F) as u64) << shift;
            if (byte & 0x80) == 0 { break; }
            shift += 7;
        }
        (value, pos)
    }

    async fn write_varint<W>(writer: &mut W, mut value: u64) -> crate::error::Result<()>
    where
        W: tokio::io::AsyncWriteExt + Unpin,
    {
        loop {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;
            if value > 0 { byte |= 0x80; }
            writer.write_all(&[byte]).await?;
            if value == 0 { break; }
        }
        Ok(())
    }
}
