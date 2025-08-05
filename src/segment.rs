use crate::{error::Result, types::*};
use memmap2::{Mmap, MmapOptions};
use std::{fs::File, path::Path};
use bytemuck::cast_slice;

pub struct SegmentFile {
    mmap: Mmap,
    header: SegmentHeader,
}

impl SegmentFile {
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        
        // Read header
        let header_bytes = &mmap[0..std::mem::size_of::<SegmentHeader>()];
        let header = *bytemuck::from_bytes::<SegmentHeader>(header_bytes);
        
        // Validate magic
        if &header.magic != b"ESEG" {
            return Err(crate::Error::InvalidSegmentFormat);
        }
        
        Ok(Self { mmap, header })
    }
    
    pub fn block_range(&self) -> (u64, u64) {
        (self.header.block_from, self.header.block_to)
    }
    
    pub fn get_index_entries(&self) -> &[IndexEntry] {
        let start = self.header.index_offset as usize;
        let size = self.header.index_count as usize * std::mem::size_of::<IndexEntry>();
        let bytes = &self.mmap[start..start + size];
        cast_slice(bytes)
    }
    
    pub fn find_block(&self, block_number: u64) -> Option<&IndexEntry> {
        let entries = self.get_index_entries();
        entries
            .binary_search_by_key(&block_number, |e| e.block_number)
            .ok()
            .map(|idx| &entries[idx])
    }
    
    pub fn read_raw_data(&self, entry: &IndexEntry) -> &[u8] {
        let start = entry.file_offset as usize;
        let end = start + entry.compressed_size as usize;
        &self.mmap[start..end]
    }
}

pub struct SegmentReader {
    segments: Vec<SegmentFile>,
}

impl SegmentReader {
    pub fn new(segments_dir: &Path) -> Result<Self> {
        let mut segments = Vec::new();
        
        // Load all .seg files in the directory
        for entry in std::fs::read_dir(segments_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.extension().and_then(|s| s.to_str()) == Some("seg") {
                match SegmentFile::open(&path) {
                    Ok(segment) => segments.push(segment),
                    Err(e) => {
                        tracing::warn!("Failed to open segment file {:?}: {}", path, e);
                    }
                }
            }
        }
        
        // Sort by block range for efficient lookup
        segments.sort_by_key(|s| s.header.block_from);
        
        Ok(Self { segments })
    }
    
    pub fn find_segment_for_block(&self, block_number: u64) -> Option<&SegmentFile> {
        self.segments
            .iter()
            .find(|seg| {
                block_number >= seg.header.block_from && block_number <= seg.header.block_to
            })
    }
    
    pub fn has_block(&self, block_number: u64) -> bool {
        self.find_segment_for_block(block_number)
            .and_then(|seg| seg.find_block(block_number))
            .is_some()
    }
}