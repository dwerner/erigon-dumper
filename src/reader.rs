use crate::{error::{Error, Result}, segment::SegmentReader, types::*};
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
        
        let header_segments = SegmentReader::new(&snapshots_path.join("headers"))?;
        let body_segments = SegmentReader::new(&snapshots_path.join("bodies"))?;
        
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
        
        // Find the index entry
        let entry = segment
            .find_block(block_number)
            .ok_or(Error::BlockNotFound(block_number))?;
        
        // Read the compressed data
        let compressed_data = segment.read_raw_data(entry);
        
        // Decompress
        let decompressed = decompress_data(compressed_data, entry)?;
        
        // Decode the header
        let mut buf = decompressed.as_slice();
        ConsensusHeader::decode(&mut buf).map_err(Into::into)
    }
    
    pub fn read_block(&self, block_number: u64) -> Result<Block> {
        // Read header
        let header = self.read_header(block_number)?;
        
        // Find body segment
        let body_segment = self.body_segments
            .find_segment_for_block(block_number)
            .ok_or(Error::BlockNotFound(block_number))?;
        
        let body_entry = body_segment
            .find_block(block_number)
            .ok_or(Error::BlockNotFound(block_number))?;
        
        // Read and decompress body
        let compressed_body = body_segment.read_raw_data(body_entry);
        let body_data = decompress_data(compressed_body, body_entry)?;
        
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
}

// Decompression function using our implementation
fn decompress_data(compressed: &[u8], entry: &IndexEntry) -> Result<Vec<u8>> {
    // Check if data is actually compressed
    if entry.flags & 1 == 0 {
        // Not compressed, return as-is
        return Ok(compressed.to_vec());
    }
    
    // Use our decompressor directly on the byte slice
    let decompressor = crate::decompress::Decompressor::new(compressed)?;
    let mut getter = decompressor.make_getter();
    let mut result = Vec::with_capacity(entry.uncompressed_size as usize);
    let mut word_buf = Vec::new();
    
    // Decompress all words
    while result.len() < entry.uncompressed_size as usize {
        let word = getter.next(&mut word_buf)?;
        if word.is_empty() {
            break;
        }
        result.extend_from_slice(&word);
    }
    
    Ok(result)
}

fn parse_block_body(data: &[u8]) -> Result<Vec<Transaction>> {
    // TODO: Implement proper block body parsing
    // Block body contains transactions and uncles in RLP format
    Ok(vec![])
}