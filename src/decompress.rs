use crate::error::{Error, Result};

/// Erigon segment decompressor implementation
/// Based on the dictionary compression algorithm used in erigon-lib/compress
/// 
/// Reference implementation:
/// https://github.com/erigontech/erigon-lib/blob/main/compress/decompress.go
/// 
/// The compression format uses:
/// - Dictionary-based pattern matching for common byte sequences
/// - Position encoding for pattern locations
/// - Uncompressed data for unique bytes
/// - Variable-length encoding for positions and patterns

const CONDENSED_PATTERN_TABLE_BIT_THRESHOLD: u8 = 9;

#[derive(Debug)]
pub struct Decompressor<'a> {
    data: &'a [u8],
    words_count: u64,
    empty_words_count: u64,
    patterns: PatternTable,
    positions: PositionTable,
    words_start: usize,
}

#[derive(Debug)]
struct PatternTable {
    bit_len: u8,
    patterns: Vec<Codeword>,
}

#[derive(Debug)]
struct PositionTable {
    bit_len: u8,
    positions: Vec<u64>,
}

#[derive(Debug)]
struct Codeword {
    pattern: Option<Vec<u8>>,
    table_ptr: Option<Box<PatternTable>>,
    code: u16,
    len: u8,
}

pub struct Getter<'a> {
    decompressor: &'a Decompressor<'a>,
    data_pos: usize,
    data_bit: u8,
}

impl<'a> Decompressor<'a> {
    /// Create a decompressor from a byte slice
    pub fn new(data: &'a [u8]) -> Result<Self> {
        if data.len() < 24 {
            return Err(Error::DecompressionError("Data too short for header".to_string()));
        }
        
        // Read header
        let mut pos = 0;
        let words_count = u64::from_le_bytes(data[pos..pos+8].try_into().unwrap());
        pos += 8;
        
        let empty_words_count = u64::from_le_bytes(data[pos..pos+8].try_into().unwrap());
        pos += 8;
        
        let dict_size = u64::from_le_bytes(data[pos..pos+8].try_into().unwrap()) as usize;
        pos += 8;
        
        // Parse dictionaries from compressed data
        let (patterns, positions) = if dict_size > 0 && pos + dict_size <= data.len() {
            Self::parse_dictionaries(&data[pos..pos + dict_size])?
        } else {
            // Empty dictionaries
            (
                PatternTable {
                    bit_len: 0,
                    patterns: vec![],
                },
                PositionTable {
                    bit_len: 0,
                    positions: vec![],
                }
            )
        };
        
        let words_start = pos + dict_size;
        
        Ok(Decompressor {
            data,
            words_count,
            empty_words_count,
            patterns,
            positions,
            words_start,
        })
    }
    
    pub fn make_getter(&'a self) -> Getter<'a> {
        Getter {
            decompressor: self,
            data_pos: self.words_start,
            data_bit: 0,
        }
    }
    
    /// Parse pattern and position dictionaries from dictionary data
    fn parse_dictionaries(dict_data: &[u8]) -> Result<(PatternTable, PositionTable)> {
        // This is a simplified version - real implementation would parse
        // the actual dictionary format from Erigon
        
        // For now, return empty dictionaries
        Ok((
            PatternTable {
                bit_len: 8, // Assume 8-bit codes for now
                patterns: vec![],
            },
            PositionTable {
                bit_len: 8, // Assume 8-bit codes for now  
                positions: vec![],
            }
        ))
    }
}

impl<'a> Getter<'a> {
    /// Read bits from compressed data
    fn read_bits(&mut self, bit_len: u8) -> u16 {
        if bit_len == 0 {
            return 0;
        }
        
        let data = self.decompressor.data;
        if self.data_pos >= data.len() {
            return 0;
        }
        
        let mut code = (data[self.data_pos] >> self.data_bit) as u16;
        
        if 8 - self.data_bit < bit_len && self.data_pos + 1 < data.len() {
            code |= (data[self.data_pos + 1] as u16) << (8 - self.data_bit);
        }
        
        if 16 - self.data_bit < bit_len && self.data_pos + 2 < data.len() {
            code |= (data[self.data_pos + 2] as u16) << (16 - self.data_bit);
        }
        
        code &= (1u16 << bit_len) - 1;
        
        // Advance position
        self.data_bit += bit_len;
        self.data_pos += (self.data_bit / 8) as usize;
        self.data_bit %= 8;
        
        code
    }
    
    /// Read next position value
    fn next_pos(&mut self, clean: bool) -> u64 {
        let table = &self.decompressor.positions;
        
        // Empty dictionary case - read raw byte
        if table.bit_len == 0 {
            // Read one byte directly when no dictionary
            return self.read_bits(8) as u64;
        }
        
        // Read the code
        let code = self.read_bits(table.bit_len);
        
        // In real implementation, would look up position based on code
        // For now, return the code as position
        code as u64
    }
    
    /// Read next pattern from dictionary
    fn next_pattern(&mut self) -> Vec<u8> {
        let table = &self.decompressor.patterns;
        
        if table.bit_len == 0 {
            return vec![];
        }
        
        let code = self.read_bits(table.bit_len);
        
        // In real implementation, would look up pattern in dictionary
        // For now, return empty pattern
        vec![]
    }
    
    /// Extract next compressed word
    pub fn next(&mut self, buf: &mut Vec<u8>) -> Result<Vec<u8>> {
        buf.clear();
        
        // Read word length (first position)
        let word_len = self.next_pos(true);
        if word_len == 0 {
            return Ok(vec![]);
        }
        
        // The word length includes a zero terminator, so actual length is word_len - 1
        let actual_len = word_len.saturating_sub(1) as usize;
        if actual_len == 0 {
            // Empty word - still need to read end-of-positions marker
            let _end_marker = self.next_pos(false);
            return Ok(vec![]);
        }
        
        // Resize buffer to actual word length
        buf.resize(actual_len, 0);
        
        // Read pattern positions and insert patterns
        let mut buf_pos = 0;
        let mut pattern_positions = Vec::new();
        
        loop {
            let pos = self.next_pos(false);
            if pos == 0 {
                break;
            }
            
            // Position is relative to current position
            buf_pos += pos as usize - 1;
            if buf_pos >= buf.len() {
                break;
            }
            
            let pattern = self.next_pattern();
            if !pattern.is_empty() {
                let copy_len = pattern.len().min(buf.len() - buf_pos);
                buf[buf_pos..buf_pos + copy_len].copy_from_slice(&pattern[..copy_len]);
                pattern_positions.push((buf_pos, buf_pos + copy_len));
                buf_pos += copy_len;
            }
        }
        
        // Now fill the uncovered positions with uncompressed data
        // Sort pattern positions to process in order
        pattern_positions.sort_by_key(|&(start, _)| start);
        
        let mut uncovered_start = 0;
        for &(pattern_start, pattern_end) in &pattern_positions {
            if pattern_start > uncovered_start {
                // We have uncovered data to read
                let uncovered_len = pattern_start - uncovered_start;
                self.read_uncompressed(&mut buf[uncovered_start..pattern_start], uncovered_len)?;
            }
            uncovered_start = pattern_end;
        }
        
        // Fill any remaining uncovered data at the end
        if uncovered_start < buf.len() {
            let remaining = buf.len() - uncovered_start;
            self.read_uncompressed(&mut buf[uncovered_start..], remaining)?;
        }
        
        Ok(buf.clone())
    }
    
    /// Read uncompressed data directly from the data stream
    fn read_uncompressed(&mut self, buf: &mut [u8], len: usize) -> Result<()> {
        let data = self.decompressor.data;
        if self.data_pos + len > data.len() {
            return Err(Error::DecompressionError("Not enough data for uncompressed read".to_string()));
        }
        
        buf.copy_from_slice(&data[self.data_pos..self.data_pos + len]);
        self.data_pos += len;
        Ok(())
    }
    
    /// Skip to next word without decompressing
    pub fn skip(&mut self) -> Result<()> {
        let word_len = self.next_pos(true);
        if word_len == 0 {
            return Ok(());
        }
        
        // TODO: Implement skip logic
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    // Test data similar to Erigon's Lorem ipsum test
    const LOREM_IPSUM: &str = "Lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua";
    
    fn create_test_compressed_data() -> Vec<u8> {
        // Create a minimal valid compressed file header
        let mut data = Vec::new();
        
        // Words count
        data.extend_from_slice(&10u64.to_le_bytes());
        
        // Empty words count
        data.extend_from_slice(&0u64.to_le_bytes());
        
        // Dictionary size (minimal for now)
        data.extend_from_slice(&0u64.to_le_bytes());
        
        // No dictionary data for this test
        // No compressed words data for this test
        
        data
    }
    
    #[test]
    fn test_decompressor_new() {
        let data = create_test_compressed_data();
        let decompressor = Decompressor::new(&data).unwrap();
        
        assert_eq!(decompressor.words_count, 10);
        assert_eq!(decompressor.empty_words_count, 0);
        assert_eq!(decompressor.words_start, 24); // Header is 24 bytes
    }
    
    #[test]
    fn test_decompressor_too_short() {
        let data = vec![0u8; 20]; // Less than header size
        let result = Decompressor::new(&data);
        
        assert!(result.is_err());
        match result {
            Err(Error::DecompressionError(msg)) => {
                assert!(msg.contains("too short"));
            }
            _ => panic!("Expected DecompressionError"),
        }
    }
    
    #[test]
    fn test_getter_read_bits() {
        let mut data = create_test_compressed_data();
        // Add some test data after header
        data.extend_from_slice(&[0b10101010, 0b11001100, 0b11110000]);
        
        let decompressor = Decompressor::new(&data).unwrap();
        let mut getter = decompressor.make_getter();
        
        // Test reading various bit lengths
        let bits_4 = getter.read_bits(4);
        assert_eq!(bits_4, 0b1010); // First 4 bits of 0b10101010
        
        let bits_8 = getter.read_bits(8);
        assert_eq!(bits_8, 0b11001010); // Next 8 bits crossing byte boundary
    }
    
    #[test]
    fn test_empty_decompression() {
        let data = create_test_compressed_data();
        let decompressor = Decompressor::new(&data).unwrap();
        let mut getter = decompressor.make_getter();
        let mut buf = Vec::new();
        
        // With no actual compressed data and bit_len=0, returns empty
        let result = getter.next(&mut buf).unwrap();
        assert!(result.is_empty());
    }
    
    #[test]
    fn test_simple_word_decompression() {
        // Create compressed data with a simple word
        // Format: [header][dictionary][compressed_words]
        let mut data = Vec::new();
        
        // Header (24 bytes)
        data.extend_from_slice(&1u64.to_le_bytes()); // 1 word
        data.extend_from_slice(&0u64.to_le_bytes()); // 0 empty words
        data.extend_from_slice(&0u64.to_le_bytes()); // No dictionary
        
        // Compressed word data (no dictionary, so direct encoding):
        // Word "hello" (5 bytes) encoded as:
        // - Length: 6 (5 + 1 for zero terminator)
        // - Position: 0 (end of positions)
        // - Raw data: "hello"
        data.push(6);  // Word length + 1
        data.push(0);  // End of positions marker
        data.extend_from_slice(b"hello"); // Uncompressed data
        
        let decompressor = Decompressor::new(&data).unwrap();
        let mut getter = decompressor.make_getter();
        let mut buf = Vec::new();
        
        // Should decompress "hello"
        let result = getter.next(&mut buf).unwrap();
        assert_eq!(result, b"hello");
    }
    
    #[test]
    fn test_multiple_words_decompression() {
        // Test decompressing multiple words
        let test_words: Vec<&[u8]> = vec![b"first", b"second", b"third"];
        let mut data = Vec::new();
        
        // Header
        data.extend_from_slice(&3u64.to_le_bytes()); // 3 words
        data.extend_from_slice(&0u64.to_le_bytes()); // 0 empty words
        data.extend_from_slice(&0u64.to_le_bytes()); // No dictionary
        
        // Compress each word (simple format without dictionary)
        for word in &test_words {
            data.push((word.len() + 1) as u8); // Length + 1
            data.push(0); // End of positions
            data.extend_from_slice(word);
        }
        
        let decompressor = Decompressor::new(&data).unwrap();
        let mut getter = decompressor.make_getter();
        let mut buf = Vec::new();
        
        // Decompress and verify each word
        for expected in &test_words {
            let result = getter.next(&mut buf).unwrap();
            assert_eq!(result, *expected);
        }
    }
    
    #[test]
    fn test_empty_word_handling() {
        // Test handling of empty words and edge cases
        let mut data = Vec::new();
        
        // Header
        data.extend_from_slice(&2u64.to_le_bytes()); // 2 words
        data.extend_from_slice(&1u64.to_le_bytes()); // 1 empty word
        data.extend_from_slice(&0u64.to_le_bytes()); // No dictionary
        
        // First word: empty (length = 1 for terminator only)
        data.push(1); // Length
        data.push(0); // End of positions
        
        // Second word: "a"
        data.push(2); // Length 
        data.push(0); // End of positions
        data.push(b'a');
        
        let decompressor = Decompressor::new(&data).unwrap();
        let mut getter = decompressor.make_getter();
        let mut buf = Vec::new();
        
        // First word should be empty
        let result = getter.next(&mut buf).unwrap();
        assert_eq!(result, b"");
        
        // Second word should be "a"
        let result = getter.next(&mut buf).unwrap();
        assert_eq!(result, b"a");
    }
    
    // TODO: Add more tests once we implement the full decompression logic
    // - Test with actual dictionary compression
    // - Test pattern matching with dictionaries
    // - Test complex position encoding
    // - Test skip functionality
}