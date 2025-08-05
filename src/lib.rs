pub mod error;
pub mod types;
pub mod reader;
pub mod segment;
pub mod decompress;
#[cfg(test)]
pub mod compress;

pub use error::{Error, Result};
pub use reader::ErigonReader;

// Re-export key types from alloy
pub use alloy_rpc_types::{Block, Transaction};