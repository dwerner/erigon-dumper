use alloy_primitives::B256;
use alloy_rpc_types::{Block, Transaction, TransactionReceipt, Log};
use bytemuck::{Pod, Zeroable};

// Zero-copy index entry that can be read directly from mmap
#[repr(C)]
#[derive(Debug, Copy, Clone, Pod, Zeroable)]
pub struct IndexEntry {
    pub block_number: u64,
    pub block_hash: [u8; 32],
    pub file_offset: u64,
    pub compressed_size: u32,
    pub uncompressed_size: u32,
    pub tx_count: u32,
    pub flags: u32,
}

impl IndexEntry {
    pub fn hash(&self) -> B256 {
        B256::from(self.block_hash)
    }
}

// Zero-copy segment header
#[repr(C)]
#[derive(Debug, Copy, Clone, Pod, Zeroable)]
pub struct SegmentHeader {
    pub magic: [u8; 4],
    pub version: u32,
    pub block_from: u64,
    pub block_to: u64,
    pub index_offset: u64,
    pub index_count: u32,
    pub compression_type: u32,
}

// Raw block data before parsing - zero allocation
pub struct RawBlock<'a> {
    pub number: u64,
    pub header_data: &'a [u8],
    pub body_data: &'a [u8],
}

// For graph-node compatibility - just wrap Alloy types
pub type EthereumBlock = Block;
pub type EthereumTransaction = Transaction;
pub type EthereumReceipt = TransactionReceipt;
pub type EthereumLog = Log;