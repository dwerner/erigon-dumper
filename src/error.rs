use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Block not found: {0}")]
    BlockNotFound(u64),
    
    #[error("Invalid segment file format")]
    InvalidSegmentFormat,
    
    #[error("Decompression error: {0}")]
    DecompressionError(String),
    
    #[error("RLP decoding error: {0}")]
    RlpError(#[from] alloy_rlp::Error),
    
    #[error("Invalid index entry")]
    InvalidIndexEntry,
    
    #[error("Segment file not found: {0}")]
    SegmentNotFound(String),
}

pub type Result<T> = std::result::Result<T, Error>;