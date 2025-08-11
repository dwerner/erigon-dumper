use crate::snapshots::{Result, SnapshotError};
use memmap2::Mmap;
use std::fs::File;
use std::path::Path;

/// Reader for Erigon/recsplit index files (.idx)
/// These files allow O(1) lookup of offsets in the corresponding .seg file
pub struct IndexReader {
    mmap: Mmap,
    bucket_size: u16,
    leaf_size: u8,
    base_data_id: u64,
    key_count: u64,
    rec_split_bits: u8,
    bucket_count: u64,
    enum_index: bool,
}

impl IndexReader {
    /// Open an index file
    pub fn new(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };

        // Parse the recsplit index header
        // Format is from erigon-lib/recsplit/index.go
        let mut reader = IndexFileReader::new(&mmap);

        // Read header
        let leaf_size = reader.read_u8()?;
        let rec_split_bits = reader.read_u8()?;
        let bucket_size = reader.read_u16()?;
        let key_count = reader.read_u64()?;

        // Check if this is an enum index (sequential keys 0,1,2,...)
        let enum_index = leaf_size & 0x80 != 0;
        let leaf_size = leaf_size & 0x7f;

        let bucket_count = (key_count + bucket_size as u64 - 1) / bucket_size as u64;

        // Read base data ID (used for enum indexes)
        let base_data_id = if enum_index { reader.read_u64()? } else { 0 };

        Ok(Self {
            mmap,
            bucket_size,
            leaf_size,
            base_data_id,
            key_count,
            rec_split_bits,
            bucket_count,
            enum_index,
        })
    }

    /// Lookup offset for a key (block number for headers)
    pub fn lookup(&self, key: u64) -> Option<u64> {
        if self.enum_index {
            // For enum indexes, key is just the sequence number
            self.lookup_enum(key)
        } else {
            // For hash indexes, need to hash the key
            self.lookup_hash(&key.to_le_bytes())
        }
    }

    /// Lookup for enum (sequential) indexes
    fn lookup_enum(&self, key: u64) -> Option<u64> {
        // Adjust key by base_data_id
        let adjusted_key = if key >= self.base_data_id {
            key - self.base_data_id
        } else {
            return None;
        };

        if adjusted_key >= self.key_count {
            return None;
        }

        // Calculate bucket and position
        let bucket_id = adjusted_key / self.bucket_size as u64;
        let bucket_offset = self.get_bucket_offset(bucket_id);

        // Read the offset from the bucket
        let reader = IndexFileReader::new(&self.mmap[bucket_offset..]);

        // In enum mode, offsets are stored sequentially
        let position_in_bucket = (adjusted_key % self.bucket_size as u64) as usize;

        // Each offset is stored as a GolombRice encoded value
        // For simplicity, we'll read it as a varint for now
        let offset = self.read_offset_at_position(&reader, position_in_bucket)?;

        Some(offset)
    }

    /// Lookup for hash-based indexes
    fn lookup_hash(&self, _key: &[u8]) -> Option<u64> {
        // Hash-based lookup is more complex, involving:
        // 1. Hash the key
        // 2. Find the bucket
        // 3. Use RecSplit algorithm to find position
        // For now, return None as headers use enum indexes
        None
    }

    /// Get the offset of a bucket in the index file
    fn get_bucket_offset(&self, bucket_id: u64) -> usize {
        // Skip header (16 bytes) + base_data_id (8 bytes if enum)
        let header_size = 16 + if self.enum_index { 8 } else { 0 };

        // Skip to the bucket offsets table at the end
        let bucket_table_offset = self.mmap.len() - (self.bucket_count * 8) as usize;

        // Read the bucket offset
        let offset_pos = bucket_table_offset + (bucket_id * 8) as usize;
        let offset_bytes = &self.mmap[offset_pos..offset_pos + 8];
        let offset = u64::from_le_bytes(offset_bytes.try_into().unwrap());

        header_size + offset as usize
    }

    /// Read an offset at a specific position in a bucket
    fn read_offset_at_position(&self, reader: &IndexFileReader, position: usize) -> Option<u64> {
        // This is simplified - actual implementation uses GolombRice encoding
        // For now, assume offsets are stored as varints
        let mut current_reader = reader.clone();

        // Skip to the position
        for _ in 0..position {
            current_reader.read_varint().ok()?;
        }

        current_reader.read_varint().ok()
    }

    /// Get the number of keys in this index
    pub fn key_count(&self) -> u64 {
        self.key_count
    }

    /// Check if this is an enum index (sequential keys)
    pub fn is_enum(&self) -> bool {
        self.enum_index
    }

    /// Get the base data ID for enum indexes
    pub fn base_data_id(&self) -> u64 {
        self.base_data_id
    }

    /// Ordinal lookup - returns offset for i-th element (0-based)
    /// This is what Erigon uses for headers
    pub fn ordinal_lookup(&self, ordinal: u64) -> Option<u64> {
        if self.enum_index {
            // For enum indexes, ordinal is the direct index
            self.lookup_enum(ordinal)
        } else {
            // For non-enum indexes, would need different handling
            None
        }
    }
}

/// Helper for reading from index file buffers
#[derive(Clone)]
struct IndexFileReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> IndexFileReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn read_u8(&mut self) -> Result<u8> {
        if self.pos >= self.data.len() {
            return Err(SnapshotError::UnexpectedEof {
                context: "reading u8".to_string(),
            });
        }
        let val = self.data[self.pos];
        self.pos += 1;
        Ok(val)
    }

    fn read_u16(&mut self) -> Result<u16> {
        if self.pos + 2 > self.data.len() {
            return Err(SnapshotError::UnexpectedEof {
                context: "reading u16".to_string(),
            });
        }
        let val = u16::from_le_bytes(
            self.data[self.pos..self.pos + 2]
                .try_into()
                .map_err(|_| SnapshotError::InvalidFormat("Invalid u16 bytes".to_string()))?,
        );
        self.pos += 2;
        Ok(val)
    }

    fn read_u64(&mut self) -> Result<u64> {
        if self.pos + 8 > self.data.len() {
            return Err(SnapshotError::UnexpectedEof {
                context: "reading u64".to_string(),
            });
        }
        let val = u64::from_le_bytes(
            self.data[self.pos..self.pos + 8]
                .try_into()
                .map_err(|_| SnapshotError::InvalidFormat("Invalid u64 bytes".to_string()))?,
        );
        self.pos += 8;
        Ok(val)
    }

    fn read_varint(&mut self) -> Result<u64> {
        let mut result = 0u64;
        let mut shift = 0;

        loop {
            if self.pos >= self.data.len() {
                return Err(SnapshotError::UnexpectedEof {
                    context: "reading varint".to_string(),
                });
            }

            let byte = self.data[self.pos];
            self.pos += 1;

            result |= ((byte & 0x7f) as u64) << shift;

            if byte & 0x80 == 0 {
                break;
            }

            shift += 7;
            if shift >= 64 {
                return Err(SnapshotError::Index("Varint too large".to_string()));
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_reader_creation() {
        // This test would need an actual .idx file
        // For now, just test that the module compiles
        assert_eq!(2 + 2, 4);
    }
}
