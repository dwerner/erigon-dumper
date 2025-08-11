use thiserror::Error;

#[derive(Error, Debug)]
pub enum SnapshotError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Decompression error: {0}")]
    Decompression(String),

    #[error("Index file error: {0}")]
    Index(String),

    #[error("RLP decoding error: {0}")]
    Rlp(#[from] alloy_rlp::Error),

    #[error("Invalid snapshot format: {0}")]
    InvalidFormat(String),

    #[error("Block not found: {0}")]
    BlockNotFound(u64),

    #[error("Index not available")]
    IndexNotAvailable,

    #[error("Hash mismatch: expected {expected:?}, got {actual:?}")]
    HashMismatch {
        expected: alloy_primitives::B256,
        actual: alloy_primitives::B256,
    },

    #[error("Invalid file path: {0}")]
    InvalidPath(String),

    #[error("Unexpected EOF while reading {context}")]
    UnexpectedEof { context: String },
}

pub type Result<T> = std::result::Result<T, SnapshotError>;
