/// Erigon segment compressor implementation
/// Based on the dictionary compression algorithm used in erigon-lib/compress
/// 
/// Reference implementation:
/// https://github.com/erigontech/erigon-lib/blob/main/compress/compress.go
///
/// This is primarily used for testing the decompressor and understanding
/// the compression format.

use crate::error::{Error, Result};
use std::collections::HashMap;

pub struct Compressor {
    words: Vec<Vec<u8>>,
    dict: HashMap<Vec<u8>, usize>,
}

impl Compressor {
    pub fn new() -> Self {
        Self {
            words: Vec::new(),
            dict: HashMap::new(),
        }
    }
    
    /// Add a word to be compressed
    pub fn add_word(&mut self, word: Vec<u8>) {
        self.words.push(word);
    }
    
    /// Compress all added words into the Erigon format
    pub fn compress(&self) -> Result<Vec<u8>> {
        let mut output = Vec::new();
        
        // Write header
        output.extend_from_slice(&(self.words.len() as u64).to_le_bytes()); // words_count
        output.extend_from_slice(&0u64.to_le_bytes()); // empty_words_count
        output.extend_from_slice(&0u64.to_le_bytes()); // dictionary_size (0 for simple implementation)
        
        // Write compressed words (simple implementation without dictionary)
        for word in &self.words {
            // Write word length + 1 (for zero terminator)
            output.push((word.len() + 1) as u8);
            
            // Write end of positions marker
            output.push(0);
            
            // Write the raw word data
            output.extend_from_slice(word);
        }
        
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decompress::Decompressor;
    
    #[test]
    fn test_compress_decompress_roundtrip() {
        let test_words = vec![
            b"hello".to_vec(),
            b"world".to_vec(),
            b"erigon".to_vec(),
            b"compression".to_vec(),
        ];
        
        // Compress
        let mut compressor = Compressor::new();
        for word in &test_words {
            compressor.add_word(word.clone());
        }
        
        let compressed = compressor.compress().unwrap();
        
        // Decompress
        let decompressor = Decompressor::new(&compressed).unwrap();
        let mut getter = decompressor.make_getter();
        let mut buf = Vec::new();
        
        // Verify each word
        for expected_word in &test_words {
            let decompressed = getter.next(&mut buf).unwrap();
            assert_eq!(&decompressed, expected_word, 
                      "Expected {:?}, got {:?}", 
                      String::from_utf8_lossy(expected_word),
                      String::from_utf8_lossy(&decompressed));
        }
        
        // Should return empty when no more words
        let result = getter.next(&mut buf).unwrap();
        assert!(result.is_empty());
    }
}