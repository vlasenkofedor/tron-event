use thiserror::Error;

#[derive(Error, Debug)]
pub enum NodeError {
    #[error("IO Error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Protobuf Decode Error: {0}")]
    ProstDecode(#[from] prost::DecodeError),

    #[error("Protobuf Encode Error: {0}")]
    ProstEncode(#[from] prost::EncodeError),

    #[error("Hex Decode Error: {0}")]
    Hex(#[from] hex::FromHexError),

    #[error("Timeout Error")]
    Timeout(#[from] tokio::time::error::Elapsed),

    #[error("Network Error: {0}")]
    Network(String),

    #[error("Unknown Error: {0}")]
    Unknown(String),
}

pub type Result<T> = std::result::Result<T, NodeError>;
