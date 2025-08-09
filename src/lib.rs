pub mod error;
pub mod types;
pub mod reader;
pub mod segment;
pub mod decompress;
pub mod compress;
pub mod compress_go_port;
pub mod compress_go_port2;
pub mod compress_go_port3;
pub mod patricia;
pub mod dictionary_builder;

#[cfg(test)]
mod test_sorting;

pub use error::{Error, Result};
pub use reader::ErigonReader;

// Re-export key types from alloy
pub use alloy_rpc_types::{Block, Transaction};