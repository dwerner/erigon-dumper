// Error types for compression/decompression operations
// Port of error handling from Go code

use thiserror::Error;
use std::io;

#[derive(Error, Debug)]
pub enum CompressionError {
    // IO related errors
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    
    // File operations
    #[error("Failed to create file: {path}: {source}")]
    FileCreate {
        path: String,
        #[source]
        source: io::Error,
    },
    
    #[error("Failed to rename file from {from} to {to}: {source}")]
    FileRename {
        from: String,
        to: String,
        #[source]
        source: io::Error,
    },
    
    #[error("Failed to create intermediate file: {0}")]
    IntermediateFileCreate(io::Error),
    
    #[error("Failed to return to start of intermediate file: {0}")]
    IntermediateFileSeek(io::Error),
    
    // Compression specific
    #[error("Invalid compression type: {0}")]
    InvalidCompressionType(String),
    
    #[error("Compression ratio calculation failed: {0}")]
    RatioCalculation(String),
    
    #[error("Pattern not found in dictionary")]
    PatternNotFound,
    
    #[error("Dictionary is empty")]
    EmptyDictionary,
    
    #[error("Word too large: {size} bytes (max: {max})")]
    WordTooLarge {
        size: usize,
        max: usize,
    },
    
    // Decompression specific
    #[error("Invalid compressed file format")]
    InvalidFormat,
    
    #[error("Corrupted compressed data")]
    CorruptedData,
    
    #[error("Unexpected end of file")]
    UnexpectedEof,
    
    // Configuration errors
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
    
    #[error("Minimum pattern length ({min}) cannot be greater than maximum ({max})")]
    InvalidPatternLengthRange {
        min: usize,
        max: usize,
    },
    
    // ETL/Collection errors
    #[error("Collector error: {0}")]
    CollectorError(String),
    
    // General errors
    #[error("Operation cancelled")]
    Cancelled,
    
    #[error("Not implemented: {0}")]
    NotImplemented(String),
    
    #[error("{0}")]
    Other(String),
}

// Convenience type alias
pub type Result<T> = std::result::Result<T, CompressionError>;

// Helper functions to create common errors (matching Go patterns)
impl CompressionError {
    pub fn io_error(msg: &str) -> Self {
        CompressionError::Other(msg.to_string())
    }
    
    pub fn wrap_io(err: io::Error, context: &str) -> Self {
        CompressionError::Other(format!("{}: {}", context, err))
    }
}