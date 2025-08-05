use crate::{error::{Error, Result}, segment::SegmentReader};
use alloy_consensus::Header as ConsensusHeader;
use alloy_rlp::Decodable;
use alloy_rpc_types::{Block, BlockTransactions, Header, Transaction};
use std::path::Path;

pub struct ErigonReader {
    header_segments: SegmentReader,
    body_segments: SegmentReader,
    // Add more segment types as needed
}

impl ErigonReader {
    pub fn open(chaindata_path: &Path) -> Result<Self> {
        let snapshots_path = chaindata_path.join("snapshots");
        
        // Try to load segments directly from snapshots directory, filtering by type
        let header_segments = SegmentReader::new_filtered(&snapshots_path, Some("headers"))?;
        let body_segments = SegmentReader::new_filtered(&snapshots_path, Some("bodies"))?;
        
        Ok(Self {
            header_segments,
            body_segments,
        })
    }
    
    pub fn has_block(&self, block_number: u64) -> bool {
        self.header_segments.has_block(block_number)
    }
    
    pub fn read_header(&self, block_number: u64) -> Result<ConsensusHeader> {
        // Find the segment containing this block
        let segment = self.header_segments
            .find_segment_for_block(block_number)
            .ok_or(Error::BlockNotFound(block_number))?;
        
        // Get the offset from the index
        let offset = segment
            .find_block(block_number)
            .ok_or(Error::BlockNotFound(block_number))?;
        
        // Read and decompress the data at this offset
        let decompressed = segment.read_compressed_data(offset)?;
        
        // The header format has a first byte followed by RLP data
        if decompressed.is_empty() {
            return Err(Error::InvalidFormat("Empty header data".into()));
        }
        
        // Skip the first byte and decode the header from the remaining RLP data
        let rlp_data = &decompressed[1..];
        ConsensusHeader::decode(&mut &rlp_data[..]).map_err(Into::into)
    }
    
    pub fn read_block(&self, block_number: u64) -> Result<Block> {
        // Read header
        let header = self.read_header(block_number)?;
        
        // Find body segment
        let body_segment = self.body_segments
            .find_segment_for_block(block_number)
            .ok_or(Error::BlockNotFound(block_number))?;
        
        let body_offset = body_segment
            .find_block(block_number)
            .ok_or(Error::BlockNotFound(block_number))?;
        
        // Read and decompress body
        let body_data = body_segment.read_compressed_data(body_offset)?;
        
        // Parse transactions from body
        let transactions = parse_block_body(&body_data)?;
        
        // Construct the block
        // Note: alloy_rpc_types::Block has a different structure than consensus Block
        // We need to convert from consensus types to RPC types
        Ok(Block {
            header: Header {
                inner: header.clone(),
                hash: header.hash_slow(),
                size: Some(alloy_primitives::U256::from(body_data.len())),
                total_difficulty: None,
            },
            uncles: vec![], // TODO: Parse uncles
            transactions: BlockTransactions::Full(transactions),
            withdrawals: None, // TODO: Handle post-merge withdrawals
        })
    }
    
    pub fn read_block_range(&self, from: u64, to: u64) -> impl Iterator<Item = Result<Block>> + '_ {
        (from..=to).map(move |num| self.read_block(num))
    }
    
    pub fn debug_segments(&self) {
        println!("Header segments loaded: {}", self.header_segments.segments.len());
        for (i, segment) in self.header_segments.segments.iter().enumerate() {
            let (from, to) = segment.block_range();
            println!("  Header segment {}: blocks {} to {}", i, from, to);
        }
        
        println!("Body segments loaded: {}", self.body_segments.segments.len());
        for (i, segment) in self.body_segments.segments.iter().enumerate() {
            let (from, to) = segment.block_range();
            println!("  Body segment {}: blocks {} to {}", i, from, to);
        }
    }
}

fn parse_block_body(_data: &[u8]) -> Result<Vec<Transaction>> {
    // TODO: Implement proper block body parsing
    // Block body contains transactions and uncles in RLP format
    Ok(vec![])
}