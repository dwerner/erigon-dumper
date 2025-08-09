// Port of Erigon's parallel_compress.go
// Original: go/src/parallel_compress.go

use crate::compress::{CompressionWord, Pattern, Position, Ring};
use crate::error::CompressionError;
use radix_trie::Trie;

// From Go: CompressionQueue type
// Go: parallel_compress.go:211
pub type CompressionQueue = Vec<CompressionWord>;

// Helper struct for pattern matching
// Replaces patricia.MatchFinder2 from Go
pub struct MatchFinder {
    trie: Trie<Vec<u8>, usize>, // Maps pattern to pattern index
}

impl MatchFinder {
    pub fn new() -> Self {
        MatchFinder { trie: Trie::new() }
    }

    pub fn insert(&mut self, pattern: Vec<u8>, index: usize) {
        self.trie.insert(pattern, index);
    }

    // Find all patterns that match at current position in input
    pub fn find_matches(&self, input: &[u8], pos: usize) -> Vec<(usize, usize)> {
        // Returns Vec of (pattern_index, pattern_length)
        let mut matches = Vec::new();

        // Check all possible pattern lengths at current position
        for len in 1..=input.len().saturating_sub(pos) {
            if let Some(&pattern_idx) = self.trie.get(&input[pos..pos + len]) {
                matches.push((pattern_idx, len));
            }
        }

        matches
    }
}

// From Go: coverWordByPatterns function
// Go: parallel_compress.go:42
pub fn cover_word_by_patterns(
    trace: bool,
    input: &[u8],
    match_finder: &MatchFinder,
    output: &mut Vec<u8>,
    uncovered: &mut Vec<usize>,
    patterns: &mut Vec<usize>,
    cell_ring: &mut Ring,
    pos_map: &std::collections::HashMap<u64, u64>,
) -> (Vec<u8>, Vec<usize>, Vec<usize>) {
    // Go: parallel_compress.go:42-177
    // This is the dynamic programming algorithm for optimal pattern coverage

    // TODO: Implement the DP algorithm
    // For now, return the inputs unchanged
    (output.clone(), uncovered.clone(), patterns.clone())
}

// From Go: Huffman tree building for patterns
// Go: parallel_compress.go:433-524
pub struct PatternHuffBuilder {
    patterns: Vec<Pattern>,
}

impl PatternHuffBuilder {
    pub fn new(patterns: Vec<Pattern>) -> Self {
        PatternHuffBuilder { patterns }
    }

    pub fn build_huffman_codes(&mut self) {
        // Go: parallel_compress.go:454-524
        // Build Huffman tree and assign codes to patterns

        // Sort patterns by uses (frequency)
        self.patterns.sort_by(|a, b| a.uses.cmp(&b.uses));

        // TODO: Implement actual Huffman tree construction
        // For now, assign simple codes
        for (i, pattern) in self.patterns.iter_mut().enumerate() {
            pattern.code = i as u64;
            // Make sure we have at least 1 bit for code
            pattern.code_bits = if i == 0 {
                1
            } else {
                (64 - (i as u64).leading_zeros()) as usize
            };
            pattern.depth = pattern.code_bits;
        }
    }
}

// From Go: Huffman tree building for positions
// Go: parallel_compress.go:533-625
pub struct PositionHuffBuilder {
    positions: Vec<Position>,
}

impl PositionHuffBuilder {
    pub fn new(positions: Vec<Position>) -> Self {
        PositionHuffBuilder { positions }
    }

    pub fn build_huffman_codes(&mut self) {
        // Go: parallel_compress.go:554-625
        // Build Huffman tree and assign codes to positions

        // Sort positions by uses (frequency)
        self.positions.sort_by(|a, b| a.uses.cmp(&b.uses));

        // TODO: Implement actual Huffman tree construction
        // For now, assign simple codes
        for (i, pos) in self.positions.iter_mut().enumerate() {
            pos.code = i as u64;
            // Make sure we have at least 1 bit for code
            pos.code_bits = if i == 0 {
                1
            } else {
                (64 - (i as u64).leading_zeros()) as usize
            };
            pos.depth = pos.code_bits;
        }
    }
}

// From Go: compressWithPatternCandidates function (main compression pipeline)
// Go: parallel_compress.go:238
pub fn compress_with_pattern_candidates(
    trace: bool,
    cfg: &crate::compress::Cfg,
    log_prefix: &str,
    segment_file_path: &str,
    cf: &mut std::fs::File,
    uncompressed_file: &mut crate::compress::RawWordsFile,
    dict_builder: &crate::compress::DictionaryBuilder,
) -> std::result::Result<(), CompressionError> {
    use std::collections::HashMap;
    use std::io::{BufWriter, Write};
    use std::fs::File;
    
    // Go: parallel_compress.go:243-255
    // Build pattern dictionary and trie
    let mut match_finder = MatchFinder::new();
    let mut code2pattern = Vec::with_capacity(256);
    
    dict_builder.for_each(|score, word| {
        let mut pattern = Pattern::new(word.to_vec(), score);
        pattern.code = code2pattern.len() as u64;
        pattern.uses = 0;
        pattern.code_bits = 0;
        
        match_finder.insert(word.to_vec(), code2pattern.len());
        code2pattern.push(pattern);
    });
    
    if cfg.workers > 1 {
        // Multi-worker mode not yet implemented
        log::warn!("[{}] Multi-worker compression not yet implemented, using single worker", log_prefix);
    }
    
    // Go: parallel_compress.go:296-303
    // Create intermediate file for first pass
    let intermediate_path = format!("{}.tmp", segment_file_path);
    let intermediate_file = File::create(&intermediate_path)?;
    let mut intermediate_w = BufWriter::new(intermediate_file);
    
    // Position map for uncompressed words
    let mut uncomp_pos_map: HashMap<u64, u64> = HashMap::new();
    
    // Variables for single-worker mode
    let mut output = Vec::with_capacity(256);
    let mut uncovered = vec![0; 256];
    let mut patterns = Vec::with_capacity(256);
    let mut cell_ring = Ring::new();
    
    let mut input_size = 0u64;
    let mut output_size = 0u64;
    let mut out_count = 0u64;
    let total_words = uncompressed_file.count;
    
    // Go: parallel_compress.go:309-410
    // Process each word
    uncompressed_file.for_each(|v, compression| {
        out_count += 1;
        let word_len = v.len() as u64;
        
        // Write length prefix
        let mut num_buf = [0u8; 10];
        let n = encode_varint(&mut num_buf, word_len);
        intermediate_w.write_all(&num_buf[..n]).ok();
        
        if word_len > 0 {
            if compression {
                // Go: parallel_compress.go:376
                // Apply pattern compression
                let (compressed, _uncovered, _patterns) = cover_word_by_patterns(
                    trace,
                    v,
                    &match_finder,
                    &mut output,
                    &mut uncovered,
                    &mut patterns,
                    &mut cell_ring,
                    &uncomp_pos_map,
                );
                intermediate_w.write_all(&compressed).ok();
                output_size += compressed.len() as u64;
            } else {
                // Go: parallel_compress.go:382-388
                // No compression - write 0 byte + raw word
                intermediate_w.write_all(&[0]).ok();
                intermediate_w.write_all(v).ok();
                output_size += 1 + v.len() as u64;
            }
        }
        
        input_size += 1 + word_len;
        *uncomp_pos_map.entry(word_len + 1).or_insert(0) += 1;
        *uncomp_pos_map.entry(0).or_insert(0) += 1;
        
        // Progress logging
        if out_count % 100000 == 0 {
            log::trace!(
                "[{}] Compression preprocessing: {:.2}%",
                log_prefix,
                100.0 * out_count as f64 / total_words as f64
            );
        }
        
        Ok(())
    })?;
    
    // Flush intermediate file
    intermediate_w.flush()?;
    drop(intermediate_w);
    
    // Go: parallel_compress.go:453-730
    // Build Huffman codes and write final compressed file
    
    // Count pattern uses
    for pattern in &mut code2pattern {
        // TODO: Count actual pattern uses during compression
        pattern.uses = pattern.score; // Use score as proxy for now
    }
    
    // Build Huffman codes for patterns
    let mut pattern_huff = PatternHuffBuilder::new(code2pattern.clone());
    pattern_huff.build_huffman_codes();
    
    // Build Huffman codes for positions
    let mut positions = Vec::new();
    for (pos, &uses) in &uncomp_pos_map {
        positions.push(Position {
            uses,
            pos: *pos,
            code: 0,
            code_bits: 0,
            depth: 0,
        });
    }
    let mut position_huff = PositionHuffBuilder::new(positions);
    position_huff.build_huffman_codes();
    
    // Write final compressed file
    write_compressed_file(
        cf,
        &intermediate_path,
        &pattern_huff.patterns,
        &position_huff.positions,
    )?;
    
    // Clean up intermediate file
    std::fs::remove_file(&intermediate_path).ok();
    
    log::info!(
        "[{}] Compression complete: input_size={}, output_size={}, ratio={:.2}",
        log_prefix,
        input_size,
        output_size,
        input_size as f64 / output_size as f64
    );
    
    Ok(())
}

// Helper to encode varint (matches Go's binary.PutUvarint)
fn encode_varint(buf: &mut [u8], mut x: u64) -> usize {
    let mut i = 0;
    while x >= 0x80 {
        buf[i] = (x as u8) | 0x80;
        x >>= 7;
        i += 1;
    }
    buf[i] = x as u8;
    i + 1
}

// Write the final compressed file with Huffman tables
fn write_compressed_file(
    cf: &mut std::fs::File,
    intermediate_path: &str,
    patterns: &[Pattern],
    positions: &[Position],
) -> std::result::Result<(), CompressionError> {
    use std::io::{BufReader, BufWriter, Read, Write};
    
    let mut w = BufWriter::new(cf);
    
    // TODO: Write header with Huffman tables
    // This would include pattern and position Huffman trees
    
    // For now, just copy intermediate file
    let intermediate = std::fs::File::open(intermediate_path)?;
    let mut reader = BufReader::new(intermediate);
    let mut buffer = vec![0u8; 8192];
    
    loop {
        let n = reader.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        w.write_all(&buffer[..n])?;
    }
    
    w.flush()?;
    Ok(())
}

// From Go: extractPatternsInSuperstrings function
// Go: parallel_compress.go:744
pub fn extract_patterns_in_superstrings(
    superstrings: Vec<Vec<u8>>,
    cfg: &crate::compress::Cfg,
) -> Vec<Pattern> {
    // Go: parallel_compress.go:744-824
    // Uses suffix array to find repeated patterns in superstrings

    let mut patterns = Vec::new();

    for superstring in superstrings {
        // Use suffix crate to build suffix array
        // DEVIATION: suffix crate requires string, not bytes
        // Converting bytes to string (assuming valid UTF-8 or using lossy conversion)
        let string_data = String::from_utf8_lossy(&superstring);
        let suffix_array = suffix::SuffixTable::new(string_data);

        // TODO: Extract patterns from suffix array
        // For now, just return empty
    }

    patterns
}

// From Go: Worker pool for parallel compression
// Go: parallel_compress.go:181
pub struct CompressionWorker {
    id: usize,
    trie: MatchFinder,
    input_size: usize,
    output_size: usize,
    pos_map: std::collections::HashMap<u64, u64>,
}

impl CompressionWorker {
    pub fn new(id: usize, patterns: &[Pattern]) -> Self {
        let mut trie = MatchFinder::new();

        // Build trie from patterns
        for (idx, pattern) in patterns.iter().enumerate() {
            trie.insert(pattern.word.clone(), idx);
        }

        CompressionWorker {
            id,
            trie,
            input_size: 0,
            output_size: 0,
            pos_map: std::collections::HashMap::new(),
        }
    }

    pub fn process_word(&mut self, word: CompressionWord) -> CompressionWord {
        // Go: parallel_compress.go:187-203
        // Process a single word for compression

        // TODO: Implement actual compression logic
        word
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_match_finder() {
        let mut mf = MatchFinder::new();
        mf.insert(b"hello".to_vec(), 0);
        mf.insert(b"world".to_vec(), 1);

        let input = b"hello world";
        let matches = mf.find_matches(input, 0);
        assert!(!matches.is_empty());
    }

    #[test]
    fn test_pattern_huff_builder() {
        let mut patterns = vec![
            Pattern::new(b"test1".to_vec(), 100),
            Pattern::new(b"test2".to_vec(), 200),
        ];
        // Set uses so code assignment works
        patterns[0].uses = 10;
        patterns[1].uses = 20;

        let mut builder = PatternHuffBuilder::new(patterns);
        builder.build_huffman_codes();

        // Verify codes were assigned
        assert!(builder.patterns[0].code_bits > 0);
        assert!(builder.patterns[1].code_bits > 0);
    }

    #[test]
    fn test_compression_worker() {
        let patterns = vec![
            Pattern::new(b"pattern1".to_vec(), 100),
            Pattern::new(b"pattern2".to_vec(), 200),
        ];

        let mut worker = CompressionWorker::new(0, &patterns);
        let word = CompressionWord::new(b"test".to_vec(), 1);
        let processed = worker.process_word(word);

        assert_eq!(processed.word, b"test");
    }
}
