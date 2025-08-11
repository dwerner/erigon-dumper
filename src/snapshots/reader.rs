use crate::decompress::{Decompressor, Getter};
use crate::snapshots::{Result, SnapshotError};
use alloy_consensus::Header;
use alloy_primitives::B256;
use alloy_rlp::Decodable;
use std::path::Path;

/// Reader for headers snapshot files
/// Headers use direct Getter access without the Reader wrapper,
/// as they are single items, not key-value pairs
pub struct HeadersReader {
    decompressor: Decompressor,
    total_words: usize,
}

impl HeadersReader {
    /// Open a headers snapshot file
    pub fn new(path: &Path) -> Result<Self> {
        let decompressor =
            Decompressor::new(path).map_err(|e| SnapshotError::Decompression(e.to_string()))?;
        let total_words = decompressor.count();
        Ok(Self {
            decompressor,
            total_words,
        })
    }

    /// Get the total number of headers in this snapshot
    pub fn count(&self) -> usize {
        self.total_words
    }

    /// Create a getter for iterating through headers
    pub fn make_getter(&self) -> HeaderGetter {
        HeaderGetter {
            getter: self.decompressor.make_getter(),
            block_number: 0, // Will be set based on snapshot range
        }
    }
}

/// Iterator for reading headers from a snapshot
pub struct HeaderGetter<'a> {
    getter: Getter<'a>,
    block_number: u64,
}

impl<'a> HeaderGetter<'a> {
    /// Check if there are more headers to read
    pub fn has_next(&self) -> bool {
        self.getter.has_next()
    }

    /// Skip the next header without decoding
    pub fn skip(&mut self) {
        self.getter.skip();
        self.block_number += 1;
    }

    /// Read the next header
    pub fn next(&mut self) -> Result<(B256, Header)> {
        let (word, _offset) = self.getter.next(Vec::new());

        // Format: hash[0]_1byte + header_rlp
        if word.is_empty() {
            return Err(SnapshotError::InvalidFormat(
                "Empty word from decompressor".to_string(),
            ));
        }

        // First byte is hash[0] for indexing
        let hash_first_byte = word[0];

        // Rest is the RLP-encoded header
        let header_rlp = &word[1..];

        // Decode the header
        let header = Header::decode(&mut &header_rlp[..])?;

        // Calculate the full hash
        let hash = header.hash_slow();

        // Verify the first byte matches (sanity check)
        if hash[0] != hash_first_byte {
            return Err(SnapshotError::InvalidFormat(format!(
                "Hash first byte mismatch: expected {:02x}, got {:02x}",
                hash_first_byte, hash[0]
            )));
        }

        self.block_number += 1;

        Ok((hash, header))
    }

    /// Reset to the beginning of the snapshot
    pub fn reset(&mut self, offset: u64) {
        self.getter.reset(offset);
        self.block_number = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    #[ignore] // Run with: cargo test --ignored test_read_real_snapshot
    fn test_read_real_snapshot() {
        // This test expects a real Erigon snapshot file
        // Update the path to point to your Erigon snapshot directory
        let snapshot_path = PathBuf::from("/path/to/erigon/snapshots/v1-000000-000500-headers.seg");

        if !snapshot_path.exists() {
            eprintln!("Snapshot file not found at {:?}", snapshot_path);
            eprintln!("Please update the path to point to a real Erigon headers snapshot");
            return;
        }

        let reader = HeadersReader::new(&snapshot_path).expect("Failed to open snapshot");
        println!("Snapshot contains {} headers", reader.count());

        let mut getter = reader.make_getter();
        let mut count = 0;

        // Read first 10 headers
        while getter.has_next() && count < 10 {
            let (hash, header) = getter.next().expect("Failed to read header");
            println!(
                "Block {}: hash={:?}, parent={:?}, timestamp={}",
                header.number, hash, header.parent_hash, header.timestamp
            );
            count += 1;
        }
    }
}
