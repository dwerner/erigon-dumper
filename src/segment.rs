use crate::{error::Result, decompress::DecompressorOwned};
use std::path::Path;

pub struct SegmentFile {
    decompressor: DecompressorOwned,
    index: RecSplitIndex,
    block_range: (u64, u64),
}

impl SegmentFile {
    pub fn open(seg_path: &Path) -> Result<Self> {
        // Parse block range from filename
        let block_range = parse_block_range_from_path(seg_path)?;
        
        // Open decompressor for .seg file
        let decompressor = DecompressorOwned::open(seg_path)?;
        
        // Open RecSplit index for .idx file
        let idx_path = seg_path.with_extension("idx");
        let mut index = RecSplitIndex::open(&idx_path)?;
        
        // Set base_data_id to the start of the block range
        index.set_base_data_id(block_range.0);
        
        Ok(Self {
            decompressor,
            index,
            block_range,
        })
    }
    
    pub fn block_range(&self) -> (u64, u64) {
        self.block_range
    }
    
    pub fn find_block(&self, block_number: u64) -> Option<u64> {
        // Use RecSplit index to find offset for block
        self.index.lookup(block_number)
    }
    
    pub fn read_compressed_data(&self, offset: u64) -> Result<Vec<u8>> {
        println!("Reading compressed data at offset: {}", offset);
        
        // Create getter and seek to offset
        let mut getter = self.decompressor.make_getter();
        getter.reset(offset);
        
        if !getter.has_next() {
            return Err(crate::Error::BlockNotFound(0));
        }
        
        let mut buf = Vec::new();
        let data = getter.next(&mut buf)?;
        println!("  Decompressed {} bytes", data.len());
        if data.len() > 0 {
            println!("  First few bytes: {:02x?}", &data[..data.len().min(32)]);
        }
        Ok(data.to_vec())
    }
}

// Simple RecSplit index implementation
pub struct RecSplitIndex {
    data: Vec<u8>,
    base_data_id: u64,
    count: u64,
    offsets_start: usize,
}

impl RecSplitIndex {
    pub fn open(path: &Path) -> Result<Self> {
        use std::fs;
        let data = fs::read(path)?;
        
        if data.len() < 16 {
            return Err(crate::Error::InvalidSegmentFormat);
        }
        
        // The index format appears to be:
        // - First 8 bytes: some header/metadata
        // - Next 8 bytes: count (number of entries)
        // - Then offsets as variable-sized values
        
        let _header = u64::from_be_bytes(data[0..8].try_into().unwrap());
        let count = u64::from_be_bytes(data[8..16].try_into().unwrap());
        
        // For headers, base_data_id is typically the starting block number
        // We'll parse it from the segment filename instead
        let base_data_id = 0; // Will be set from filename
        
        Ok(Self { 
            data, 
            base_data_id,
            count,
            offsets_start: 16,
        })
    }
    
    pub fn set_base_data_id(&mut self, base: u64) {
        self.base_data_id = base;
    }
    
    pub fn lookup(&self, key: u64) -> Option<u64> {
        if key < self.base_data_id || key >= self.base_data_id + self.count {
            return None;
        }
        
        let ordinal = key - self.base_data_id;
        
        // Simple offset lookup - read 2 bytes per entry
        let offset_pos = self.offsets_start + (ordinal as usize * 2);
        if offset_pos + 2 > self.data.len() {
            return None;
        }
        
        // Read offset as big-endian u16
        let offset = u16::from_be_bytes([
            self.data[offset_pos],
            self.data[offset_pos + 1],
        ]) as u64;
        
        Some(offset)
    }
}

fn parse_block_range_from_path(path: &Path) -> Result<(u64, u64)> {
    let filename = path.file_stem()
        .and_then(|s| s.to_str())
        .ok_or(crate::Error::InvalidSegmentFormat)?;
    
    // Parse format like "v1-023070-023071-headers"
    let parts: Vec<&str> = filename.split('-').collect();
    if parts.len() < 4 {
        return Err(crate::Error::InvalidSegmentFormat);
    }
    
    let from: u64 = parts[1].parse::<u64>()
        .map_err(|_| crate::Error::InvalidSegmentFormat)?;
    let to: u64 = parts[2].parse::<u64>()
        .map_err(|_| crate::Error::InvalidSegmentFormat)?;
    
    // Erigon snapshot filenames use thousands, so multiply by 1000
    Ok((from * 1000, to * 1000))
}

pub struct SegmentReader {
    pub segments: Vec<SegmentFile>,
}

impl SegmentReader {
    pub fn new(segments_dir: &Path) -> Result<Self> {
        Self::new_filtered(segments_dir, None)
    }
    
    pub fn new_filtered(segments_dir: &Path, filter: Option<&str>) -> Result<Self> {
        let mut segments = Vec::new();
        
        // Load all .seg files in the directory
        for entry in std::fs::read_dir(segments_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.extension().and_then(|s| s.to_str()) == Some("seg") {
                // Apply filter if provided
                if let Some(filter_str) = filter {
                    if !path.file_name()
                        .and_then(|n| n.to_str())
                        .map(|n| n.contains(filter_str))
                        .unwrap_or(false)
                    {
                        continue;
                    }
                }
                
                match SegmentFile::open(&path) {
                    Ok(segment) => segments.push(segment),
                    Err(e) => {
                        tracing::warn!("Failed to open segment file {:?}: {}", path, e);
                    }
                }
            }
        }
        
        // Sort by block range for efficient lookup
        segments.sort_by_key(|s| s.block_range.0);
        
        Ok(Self { segments })
    }
    
    pub fn find_segment_for_block(&self, block_number: u64) -> Option<&SegmentFile> {
        self.segments
            .iter()
            .find(|seg| {
                let (from, to) = seg.block_range();
                block_number >= from && block_number <= to
            })
    }
    
    pub fn has_block(&self, block_number: u64) -> bool {
        self.find_segment_for_block(block_number)
            .and_then(|seg| seg.find_block(block_number))
            .is_some()
    }
}