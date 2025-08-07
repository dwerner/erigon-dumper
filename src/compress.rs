use crate::error::{Result, Error};
use std::path::{Path, PathBuf};
use std::fs::File;
use std::io::{Write, BufWriter, BufReader, Read};
use std::collections::{HashMap, BinaryHeap};
use std::cmp::Ordering;

// Cfg matches Go's Cfg struct exactly
pub struct Cfg {
    pub min_pattern_score: u64,
    pub min_pattern_len: usize,
    pub max_pattern_len: usize,
    pub sampling_factor: u64,
    pub max_dict_patterns: usize,
    pub dict_reducer_soft_limit: usize,
    pub workers: usize,
}

impl Default for Cfg {
    fn default() -> Self {
        // Match Go's DefaultCfg
        Cfg {
            min_pattern_score: 1024,
            min_pattern_len: 5,
            max_pattern_len: 128,
            sampling_factor: 4,
            max_dict_patterns: 64 * 1024,
            dict_reducer_soft_limit: 1_000_000,
            workers: 1,
        }
    }
}

// RawWordsFile matches Go's RawWordsFile struct
struct RawWordsFile {
    file: BufWriter<File>,
    file_path: PathBuf,
    count: u64,
}

impl RawWordsFile {
    fn new(file_path: &Path) -> Result<Self> {
        let file = File::create(file_path)?;
        let writer = BufWriter::new(file);
        Ok(RawWordsFile {
            file: writer,
            file_path: file_path.to_path_buf(),
            count: 0,
        })
    }

    fn append(&mut self, word: &[u8]) -> Result<()> {
        self.count += 1;
        // For compressed words, the length prefix is shifted to make lowest bit zero
        write_varint(&mut self.file, 2 * word.len() as u64)?;
        if !word.is_empty() {
            self.file.write_all(word)?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn append_uncompressed(&mut self, word: &[u8]) -> Result<()> {
        self.count += 1;
        // For uncompressed words, the length prefix is shifted to make lowest bit one
        write_varint(&mut self.file, 2 * word.len() as u64 + 1)?;
        if !word.is_empty() {
            self.file.write_all(word)?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.file.flush()?;
        Ok(())
    }

    fn close(mut self) -> Result<()> {
        self.file.flush()?;
        Ok(())
    }

    fn close_and_remove(self) -> Result<()> {
        drop(self.file);
        std::fs::remove_file(&self.file_path)?;
        Ok(())
    }
}

// word matches Go's word type ([]byte)
type Word = Vec<u8>;

// Pattern struct matching Go's Pattern
#[derive(Clone)]
struct Pattern {
    word: Vec<u8>,      // []byte
    score: u64,         // uint64
    uses: u64,          // uint64
    code: u64,          // uint64
    code_bits: usize,   // int
    depth: usize,       // int
}

// PatternHuff matches Go's PatternHuff struct (Huffman tree node)
struct PatternHuff {
    p0: Option<Box<Pattern>>,         // *Pattern
    p1: Option<Box<Pattern>>,         // *Pattern
    h0: Option<Box<PatternHuff>>,     // *PatternHuff
    h1: Option<Box<PatternHuff>>,     // *PatternHuff
    uses: u64,                        // uint64
    tie_breaker: u64,                 // uint64
}


/// Position represents a position value with its usage count
#[derive(Clone, Debug)]
struct Position {
    pos: u64,
    uses: u64,
    code: u64,
    code_bits: usize,
    depth: usize,
}

/// BitWriter for writing variable-length bit codes
struct BitWriter<W: Write> {
    writer: W,
    output_byte: u8,
    output_bits: usize,
}

impl<W: Write> BitWriter<W> {
    fn new(writer: W) -> Self {
        BitWriter {
            writer,
            output_byte: 0,
            output_bits: 0,
        }
    }

    fn write_bits(&mut self, code: u64, bits: usize) -> Result<()> {
        let mut code = code;
        let mut bits_remaining = bits;
        
        while bits_remaining > 0 {
            let bits_to_write = std::cmp::min(bits_remaining, 8 - self.output_bits);
            
            // Extract the bits we want to write
            let mask = (1u64 << bits_to_write) - 1;
            let bits_value = (code & mask) as u8;
            
            // Add to output byte
            self.output_byte |= bits_value << self.output_bits;
            self.output_bits += bits_to_write;
            
            // If byte is full, write it
            if self.output_bits == 8 {
                self.writer.write_all(&[self.output_byte])?;
                self.output_byte = 0;
                self.output_bits = 0;
            }
            
            // Move to next bits
            code >>= bits_to_write;
            bits_remaining -= bits_to_write;
        }
        
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.output_bits > 0 {
            self.writer.write_all(&[self.output_byte])?;
            self.output_byte = 0;
            self.output_bits = 0;
        }
        Ok(())
    }
}

/// Node in Huffman tree for positions
#[derive(Debug)]
struct PositionHuff {
    p0: Option<Box<Position>>,
    p1: Option<Box<Position>>,
    h0: Option<Box<PositionHuff>>,
    h1: Option<Box<PositionHuff>>,
    uses: u64,
    tie_breaker: u64,
}

impl PositionHuff {
    fn new_leaf(pos: Position, tie_breaker: u64) -> Self {
        let uses = pos.uses;
        PositionHuff {
            p0: Some(Box::new(pos)),
            p1: None,  // Leaf nodes should only have one position
            h0: None,
            h1: None,
            uses,
            tie_breaker,
        }
    }

    fn new_node(h0: PositionHuff, h1: PositionHuff, tie_breaker: u64) -> Self {
        let uses = h0.uses + h1.uses;
        PositionHuff {
            p0: None,
            p1: None,
            h0: Some(Box::new(h0)),
            h1: Some(Box::new(h1)),
            uses,
            tie_breaker,
        }
    }

    fn set_codes(&mut self, code: u64, bits: usize) {
        if let Some(p0) = &mut self.p0 {
            // Leaf node - set the code for this position
            p0.code = code;
            p0.code_bits = bits;
        } else if let (Some(h0), Some(h1)) = (&mut self.h0, &mut self.h1) {
            // Internal node - recursively set codes for children
            h0.set_codes(code << 1, bits + 1);
            h1.set_codes((code << 1) | 1, bits + 1);
        }
    }

    fn set_depth(&mut self, depth: usize) {
        if let Some(p0) = &mut self.p0 {
            // Leaf node - set depth for this position
            p0.depth = depth;
        } else if let (Some(h0), Some(h1)) = (&mut self.h0, &mut self.h1) {
            // Internal node - recursively set depth for children
            h0.set_depth(depth + 1);
            h1.set_depth(depth + 1);
        }
    }

    fn collect_positions(&self, positions: &mut Vec<Position>) {
        if let Some(p0) = &self.p0 {
            // Leaf node - collect this position
            positions.push((**p0).clone());
        } else if let (Some(h0), Some(h1)) = (&self.h0, &self.h1) {
            // Internal node - recursively collect from children
            h0.collect_positions(positions);
            h1.collect_positions(positions);
        }
    }
}

impl Eq for PositionHuff {}

impl PartialEq for PositionHuff {
    fn eq(&self, other: &Self) -> bool {
        self.uses == other.uses && self.tie_breaker == other.tie_breaker
    }
}

impl Ord for PositionHuff {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse order for min-heap (BinaryHeap is max-heap by default)
        match other.uses.cmp(&self.uses) {
            Ordering::Equal => other.tie_breaker.cmp(&self.tie_breaker),
            other => other,
        }
    }
}

impl PartialOrd for PositionHuff {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Main Compressor struct matching Go implementation
pub struct Compressor {
    cfg: Cfg,
    output_file: PathBuf,
    tmp_out_file_path: PathBuf,
    uncompressed_file: RawWordsFile,
    words_count: u64,
    empty_words_count: u64,
    superstring: Vec<u8>,
    superstring_count: u64,
    superstring_len: usize,
    // For pattern collection
    pattern_scores: HashMap<Vec<u8>, u64>,
    // For position encoding
    position_counts: HashMap<u64, u64>,
}

// Pattern match result for a single word
#[derive(Debug, Clone)]
struct PatternMatch {
    position: usize,      // Position in word where pattern starts
    pattern_id: usize,    // Index in pattern dictionary
    length: usize,        // Length of the matched pattern
}

impl Compressor {
    pub fn new(
        _log_prefix: &str,
        output_file: &Path,
        tmp_dir: &Path,
        cfg: Cfg,
    ) -> Result<Self> {
        let tmp_out_file_path = output_file.with_extension("seg.tmp");
        let uncompressed_path = tmp_dir.join(
            output_file.file_stem()
                .ok_or_else(|| Error::InvalidFormat("no file stem".into()))?
        ).with_extension("idt");
        
        let uncompressed_file = RawWordsFile::new(&uncompressed_path)?;
        
        Ok(Compressor {
            cfg,
            output_file: output_file.to_path_buf(),
            tmp_out_file_path,
            uncompressed_file,
            words_count: 0,
            empty_words_count: 0,
            superstring: Vec::new(),
            superstring_count: 0,
            superstring_len: 0,
            pattern_scores: HashMap::new(),
            position_counts: HashMap::new(),
        })
    }

    pub fn add_word(&mut self, word: &[u8]) -> Result<()> {
        self.words_count += 1;
        if word.is_empty() {
            self.empty_words_count += 1;
        }
        
        // Track position (word length + 1) for Huffman encoding
        let position = (word.len() + 1) as u64;
        *self.position_counts.entry(position).or_insert(0) += 1;
        
        let l = 2 * word.len() + 2;
        const SUPERSTRING_LIMIT: usize = 1 << 30; // Match Go's superstringLimit
        
        if self.superstring_len + l > SUPERSTRING_LIMIT {
            if self.superstring_count % self.cfg.sampling_factor == 0 {
                // Process superstring for patterns
                self.process_superstring();
            }
            self.superstring_count += 1;
            self.superstring = Vec::new();
            self.superstring_len = 0;
        }
        
        self.superstring_len += l;
        
        if self.superstring_count % self.cfg.sampling_factor == 0 {
            // Transform word into superstring format
            for &byte in word {
                self.superstring.push(1);
                self.superstring.push(byte);
            }
            self.superstring.push(0);
            self.superstring.push(0);
        }
        
        self.uncompressed_file.append(word)
    }

    pub fn add_uncompressed_word(&mut self, word: &[u8]) -> Result<()> {
        self.words_count += 1;
        self.uncompressed_file.append_uncompressed(word)
    }

    fn process_superstring(&mut self) {
        // Simple pattern extraction - just count occurrences of substrings
        let data = &self.superstring;
        let min_len = self.cfg.min_pattern_len * 2; // Each char is 2 bytes in superstring
        let max_len = std::cmp::min(self.cfg.max_pattern_len * 2, data.len());
        
        for len in (min_len..=max_len).step_by(2) {
            for i in 0..data.len().saturating_sub(len) {
                let pattern = &data[i..i + len];
                // Only consider patterns that start with 0x01 (valid character marker)
                if pattern.len() >= 2 && pattern[0] == 1 {
                    let entry = self.pattern_scores.entry(pattern.to_vec()).or_insert(0);
                    *entry += 1;
                }
            }
        }
    }

    pub fn compress(&mut self) -> Result<()> {
        self.uncompressed_file.flush()?;
        
        // Process final superstring
        if !self.superstring.is_empty() && self.superstring_count % self.cfg.sampling_factor == 0 {
            self.process_superstring();
        }
        
        // Ensure position 0 (terminator) is in position counts
        // This is needed for marking end of positions in each word
        self.position_counts.entry(0).or_insert(self.words_count);
        
        // Build dictionary from patterns first
        let mut dict = self.build_dictionary()?;
        
        // Add pattern positions that will be needed during compression
        // This mimics Go's approach in coverWordByPatterns where posMap is updated
        self.collect_pattern_positions(&mut dict)?;
        
        // Rebuild Huffman codes now that we have actual usage statistics
        self.rebuild_huffman_codes(&mut dict)?;
        
        // Write compressed file
        self.write_compressed_file(&dict)?;
        
        // Rename tmp file to final file
        std::fs::rename(&self.tmp_out_file_path, &self.output_file)?;
        
        Ok(())
    }

    fn build_dictionary(&self) -> Result<Vec<Pattern>> {
        let mut patterns = Vec::new();
        
        println!("Building dictionary from {} pattern scores, min_score={}", 
                 self.pattern_scores.len(), self.cfg.min_pattern_score);
        
        // Select patterns with score >= min_pattern_score
        for (pattern_data, &score) in &self.pattern_scores {
            if score >= self.cfg.min_pattern_score {
                // Convert from superstring format back to normal bytes
                let mut normal_pattern = Vec::new();
                let mut i = 0;
                while i < pattern_data.len() {
                    if pattern_data[i] == 1 && i + 1 < pattern_data.len() {
                        normal_pattern.push(pattern_data[i + 1]);
                        i += 2;
                    } else if pattern_data[i] == 0 && i + 1 < pattern_data.len() && pattern_data[i + 1] == 0 {
                        // End of word marker
                        break;
                    } else {
                        i += 1;
                    }
                }
                
                if !normal_pattern.is_empty() {
                    patterns.push(Pattern {
                        word: normal_pattern,
                        score,
                        depth: 0, // Will be assigned during Huffman tree building
                        uses: 1, // Initialize with 1 to ensure all patterns have some usage
                        code: 0, // Stable pattern ID - will be assigned next
                        code_bits: 0, // Will be assigned during Huffman tree building
                    });
                }
            }
        }
        
        println!("Selected {} patterns before sorting and truncation", patterns.len());
        
        // Sort by score descending (like Go's dictBuilder.ForEach order)
        patterns.sort_by(|a, b| b.score.cmp(&a.score));
        
        // Limit to max_dict_patterns
        let max_patterns = std::cmp::min(self.cfg.max_dict_patterns, 1000);
        patterns.truncate(max_patterns);
        
        println!("Final dictionary has {} patterns after truncation (max={})", 
                 patterns.len(), self.cfg.max_dict_patterns);
        
        // Assign stable pattern codes (like Go's code2pattern indices)
        // This matches Go's: code: uint64(len(code2pattern))
        let mut code_to_pattern = Vec::new();
        for (i, pattern) in patterns.iter_mut().enumerate() {
            pattern.code = i as u64; // Stable pattern ID (index in code2pattern equivalent)
            code_to_pattern.push(i); // Maps stable code -> index in patterns array
            
            println!("  Assigning stable pattern code {}: '{}'", pattern.code, String::from_utf8_lossy(&pattern.word));
        }

        // Clone patterns for Huffman processing (will be sorted by usage for Huffman tree)
        let mut huffman_patterns = patterns.clone();
        
        // Simulate pattern usage for Huffman tree building (Go uses actual usage)
        // For now, use the scores as usage approximation
        for pattern in &mut huffman_patterns {
            pattern.uses = pattern.score; // Use score as usage frequency
        }
        
        // Build Huffman tree for patterns (assigns Huffman codes and depths)
        if !huffman_patterns.is_empty() {
            self.assign_pattern_huffman_codes(&mut huffman_patterns)?;
            
            // Copy back the Huffman codes and depths to the main patterns
            for (i, huffman_pattern) in huffman_patterns.iter().enumerate() {
                patterns[i].code = huffman_pattern.code;
                patterns[i].code_bits = huffman_pattern.code_bits;
                patterns[i].depth = huffman_pattern.depth;
            }
        }

        Ok(patterns)
    }

    fn assign_pattern_huffman_codes(&self, patterns: &mut [Pattern]) -> Result<()> {
        // Build Huffman tree for pattern codes based on pattern scores (frequency)
        let mut huffman_data: Vec<(u64, u64)> = patterns.iter()
            .enumerate()
            .map(|(i, pattern)| (i as u64, pattern.score))
            .collect();

        // Sort by frequency (score) for Huffman encoding
        huffman_data.sort_by_key(|&(_, score)| std::cmp::Reverse(score));

        // Simple Huffman code assignment (binary tree approach)
        if huffman_data.len() == 1 {
            // Special case: single pattern gets 1-bit code  
            patterns[0].code = 0;
            patterns[0].code_bits = 1;
            patterns[0].depth = 1;
        } else if huffman_data.len() > 1 {
            // Use simple depth-based encoding like positions
            let total_patterns = huffman_data.len();
            let max_depth = (total_patterns as f64).log2().ceil() as u64 + 1;
            
            for (rank, &(pattern_idx, _)) in huffman_data.iter().enumerate() {
                let pattern = &mut patterns[pattern_idx as usize];
                
                // Assign depth based on rank (most frequent = shallow)
                let depth = std::cmp::min((rank / 2) as u64 + 1, max_depth);
                pattern.depth = depth as usize;
                
                // Generate binary code for this depth
                let code_bits = depth as usize;
                let huffman_code = rank as u64 % (1u64 << code_bits);
                
                pattern.code = huffman_code;
                pattern.code_bits = code_bits;
                
                if pattern_idx < 5 {
                    println!("  Pattern {}: depth={}, code={:b} (bits={}), score={}", 
                             pattern_idx, depth, huffman_code, code_bits, patterns[pattern_idx as usize].score);
                }
            }
        }

        Ok(())
    }

    fn write_compressed_file(&mut self, patterns: &[Pattern]) -> Result<()> {
        let mut file = BufWriter::new(File::create(&self.tmp_out_file_path)?);
        
        // Header: 24 bytes
        // - words_count (8 bytes, big-endian)
        file.write_all(&self.words_count.to_be_bytes())?;
        file.write_all(&self.empty_words_count.to_be_bytes())?;
        
        // Write pattern dictionary
        let dict_data = self.encode_pattern_dict(patterns)?;
        file.write_all(&(dict_data.len() as u64).to_be_bytes())?;
        file.write_all(&dict_data)?;
        
        // Write position dictionary
        let pos_dict_data = self.encode_pos_dict(patterns)?;
        file.write_all(&(pos_dict_data.len() as u64).to_be_bytes())?;
        file.write_all(&pos_dict_data)?;
        
        // Build position lookup for compression
        let pos_dict = self.build_pos_lookup()?;
        
        // Write compressed words
        self.write_compressed_words(&mut file, patterns, &pos_dict)?;
        
        file.flush()?;
        Ok(())
    }

    fn encode_pattern_dict(&self, patterns: &[Pattern]) -> Result<Vec<u8>> {
        let mut data = Vec::new();
        
        println!("Encoding pattern dictionary with {} patterns", patterns.len());
        
        // CRITICAL: Patterns must be written in the SAME ORDER that the decompressor will use
        // to rebuild the Huffman tree. In Go, this is the patternList order (sorted by frequency).
        // The decompressor builds its Huffman tree from the patterns in file order.
        
        // First, we need to sort patterns by their Huffman tree order (by uses/frequency)
        let mut sorted_patterns = patterns.to_vec();
        sorted_patterns.sort_by(|a, b| {
            // Sort by uses (frequency) ascending, with code as tiebreaker
            // This matches Go's patternListCmp
            if a.uses == b.uses {
                // Use stable code as tiebreaker (reversed like Go does)
                b.code.cmp(&a.code)
            } else {
                a.uses.cmp(&b.uses)
            }
        });
        
        // Write patterns in Huffman tree order
        for (i, pattern) in sorted_patterns.iter().enumerate() {
            if i < 5 {
                println!("  Writing pattern {} in file: stable_code={}, depth={}, uses={}, data={:?}", 
                         i, pattern.code, pattern.depth, pattern.uses,
                         String::from_utf8_lossy(&pattern.word));
            }
            write_varint_to_vec(&mut data, pattern.depth as u64)?;
            write_varint_to_vec(&mut data, pattern.word.len() as u64)?;
            data.extend_from_slice(&pattern.word);
        }
        
        println!("Pattern dictionary encoded: {} bytes", data.len());
        Ok(data)
    }

    fn encode_pos_dict(&self, _patterns: &[Pattern]) -> Result<Vec<u8>> {
        if self.position_counts.is_empty() {
            return Ok(Vec::new());
        }
        
        // Build Huffman tree for positions
        let mut positions: Vec<Position> = self.position_counts.iter()
            .map(|(&pos, &uses)| Position {
                pos,
                uses,
                code: 0,
                code_bits: 0,
                depth: 0,
            })
            .collect();
        
        // Sort positions to ensure deterministic order
        positions.sort_by_key(|p| p.pos);
        
        // Build Huffman tree
        let mut heap = BinaryHeap::new();
        let mut tie_breaker = 0u64;
        
        // Add all positions as leaf nodes
        for pos in positions {
            heap.push(PositionHuff::new_leaf(pos, tie_breaker));
            tie_breaker += 1;
        }
        
        // Build tree by combining nodes
        while heap.len() > 1 {
            let h0 = heap.pop().unwrap();
            let h1 = heap.pop().unwrap();
            heap.push(PositionHuff::new_node(h0, h1, tie_breaker));
            tie_breaker += 1;
        }
        
        // Assign codes and depths
        if let Some(mut root) = heap.pop() {
            root.set_codes(0, 0);
            root.set_depth(0);
            
            // Collect positions with assigned codes
            let mut coded_positions = Vec::new();
            root.collect_positions(&mut coded_positions);
            
            // Sort by depth, then by code for consistent encoding with decompressor
            coded_positions.sort_by(|a, b| {
                match a.depth.cmp(&b.depth) {
                    Ordering::Equal => a.code.cmp(&b.code),
                    other => other,
                }
            });
            
            // Encode as depth-position pairs
            let mut data = Vec::new();
            for pos in &coded_positions {
                println!("Encoding position entry: depth={}, pos={}", pos.depth, pos.pos);
                write_varint_to_vec(&mut data, pos.depth as u64)?;
                write_varint_to_vec(&mut data, pos.pos)?;
            }
            
            println!("Position dictionary encoded: {} bytes", data.len());
            Ok(data)
        } else {
            Ok(Vec::new())
        }
    }

    fn build_pos_lookup(&self) -> Result<HashMap<u64, Position>> {
        let mut lookup = HashMap::new();
        
        if self.position_counts.is_empty() {
            return Ok(lookup);
        }
        
        // Build Huffman tree for positions (same as in encode_pos_dict)
        let positions: Vec<Position> = self.position_counts.iter()
            .map(|(&pos, &uses)| Position {
                pos,
                uses,
                code: 0,
                code_bits: 0,
                depth: 0,
            })
            .collect();
        
        let mut heap = BinaryHeap::new();
        let mut tie_breaker = 0u64;
        
        for pos in positions {
            heap.push(PositionHuff::new_leaf(pos, tie_breaker));
            tie_breaker += 1;
        }
        
        while heap.len() > 1 {
            let h0 = heap.pop().unwrap();
            let h1 = heap.pop().unwrap();
            heap.push(PositionHuff::new_node(h0, h1, tie_breaker));
            tie_breaker += 1;
        }
        
        if let Some(mut root) = heap.pop() {
            root.set_codes(0, 0);
            root.set_depth(0);
            
            let mut coded_positions = Vec::new();
            root.collect_positions(&mut coded_positions);
            
            for pos in coded_positions {
                lookup.insert(pos.pos, pos);
            }
        }
        
        Ok(lookup)
    }


    // Cover a word with patterns - port of Go's coverWordByPatterns logic
    fn cover_word_with_patterns(&self, word: &[u8], patterns: &[Pattern]) -> Vec<PatternMatch> {
        let mut matches = Vec::new();
        
        if patterns.is_empty() || word.is_empty() {
            return matches;
        }
        
        let word_str = String::from_utf8_lossy(word);
        println!("    Checking word '{}' against {} patterns", word_str, patterns.len());
        
        // Simple pattern matching - find all occurrences of each pattern
        for (pattern_id, pattern_data) in patterns.iter().enumerate() {
            if pattern_data.word.is_empty() {
                continue;
            }
            
            let pattern_str = String::from_utf8_lossy(&pattern_data.word);
            
            // Find all occurrences of this pattern in the word
            let mut pos = 0;
            while pos + pattern_data.word.len() <= word.len() {
                if &word[pos..pos + pattern_data.word.len()] == &pattern_data.word {
                    println!("      MATCH: pattern {} ('{}') matches at pos {}", pattern_id, pattern_str, pos);
                    matches.push(PatternMatch {
                        position: pos,
                        pattern_id,
                        length: pattern_data.word.len(),
                    });
                    // Skip overlapping matches for now (can be optimized later)
                    pos += pattern_data.word.len();
                } else {
                    pos += 1;
                }
            }
        }
        
        // Sort matches by position
        matches.sort_by_key(|m| m.position);
        
        // Remove overlapping matches (keep first match at each position)
        let mut filtered_matches = Vec::new();
        let mut last_end = 0;
        
        for m in matches {
            if m.position >= last_end {
                last_end = m.position + m.length;
                filtered_matches.push(m);
            }
        }
        
        filtered_matches
    }

    fn write_compressed_words(&mut self, file: &mut BufWriter<File>, patterns: &[Pattern], pos_dict: &HashMap<u64, Position>) -> Result<()> {
        println!("Writing {} compressed words, patterns has {} entries", self.words_count, patterns.len());
        println!("Position dictionary has {} entries", pos_dict.len());
        
        // Re-open uncompressed file for reading
        self.uncompressed_file.flush()?;
        let uncompressed_file = File::open(&self.uncompressed_file.file_path)?;
        let mut reader = BufReader::new(uncompressed_file);
        
        if !pos_dict.is_empty() {
            // Build pattern Huffman codes once
            let mut sorted_patterns = patterns.to_vec();
            sorted_patterns.sort_by(|a, b| {
                if a.uses == b.uses {
                    b.code.cmp(&a.code)
                } else {
                    a.uses.cmp(&b.uses)
                }
            });
            
            self.assign_pattern_huffman_codes(&mut sorted_patterns)?;
            
            // Map from original pattern index to Huffman code
            let mut pattern_id_to_huffman = HashMap::new();
            for sorted_pattern in sorted_patterns.iter() {
                for (orig_idx, orig_pattern) in patterns.iter().enumerate() {
                    if orig_pattern.code == sorted_pattern.code {
                        pattern_id_to_huffman.insert(orig_idx, (sorted_pattern.code, sorted_pattern.code_bits));
                        break;
                    }
                }
            }
            
            // Process each word independently
            for word_idx in 0..self.words_count {
                let len_encoded = read_varint(&mut reader)?;
                let _is_compressed = (len_encoded & 1) == 0;
                let len = (len_encoded >> 1) as usize;
                
                let mut word = vec![0u8; len];
                if len > 0 {
                    reader.read_exact(&mut word)?;
                }
                
                println!("Processing word {}: {:?} (len={})", word_idx, String::from_utf8_lossy(&word), len);
                
                // Create bit writer for this word
                let mut bit_buffer = Vec::new();
                let mut bit_writer = BitWriter::new(&mut bit_buffer);
                
                // 1. Write word length + 1 as position
                let word_len_pos = (len + 1) as u64;
                if let Some(pos_entry) = pos_dict.get(&word_len_pos) {
                    println!("  Word length position {} -> code={:b} bits={}", word_len_pos, pos_entry.code, pos_entry.code_bits);
                    bit_writer.write_bits(pos_entry.code, pos_entry.code_bits)?;
                } else {
                    return Err(Error::InvalidFormat(format!("Word length position {} not in dictionary", word_len_pos)));
                }
                
                // 2. Find pattern matches in this word
                let pattern_matches = self.cover_word_with_patterns(&word, patterns);
                println!("  Found {} pattern matches", pattern_matches.len());
                
                // Pre-compute relative positions and ensure they're in the dictionary
                let mut actual_pattern_matches = Vec::new();
                let mut temp_last_pos = 0u64;
                for pattern_match in &pattern_matches {
                    let relative_pos = (pattern_match.position as u64 + 1) - temp_last_pos;
                    temp_last_pos = pattern_match.position as u64 + 1;
                    
                    if pos_dict.contains_key(&relative_pos) {
                        actual_pattern_matches.push(pattern_match.clone());
                    } else {
                        println!("    Skipping pattern {} at pos {} (relative_pos={} not in dictionary)", 
                                pattern_match.pattern_id, pattern_match.position, relative_pos);
                    }
                }
                
                // 3. Write interleaved position and pattern codes
                let mut last_pos = 0u64;
                let mut covered_positions = vec![false; word.len()];
                
                for pattern_match in &actual_pattern_matches {
                    // Write relative position
                    let relative_pos = (pattern_match.position as u64 + 1) - last_pos;
                    last_pos = pattern_match.position as u64 + 1;
                    
                    if let Some(pos_entry) = pos_dict.get(&relative_pos) {
                        println!("    Pattern {} at pos {}, relative_pos={}", pattern_match.pattern_id, pattern_match.position, relative_pos);
                        println!("      Writing relative position {} -> code={:b} bits={}", relative_pos, pos_entry.code, pos_entry.code_bits);
                        bit_writer.write_bits(pos_entry.code, pos_entry.code_bits)?;
                        
                        // Write pattern code immediately after position (interleaved)
                        if let Some(&(huffman_code, code_bits)) = pattern_id_to_huffman.get(&pattern_match.pattern_id) {
                            println!("      Writing pattern {} -> huffman_code={:b} bits={}", 
                                     pattern_match.pattern_id, huffman_code, code_bits);
                            bit_writer.write_bits(huffman_code, code_bits)?;
                        } else {
                            return Err(Error::InvalidFormat(format!("Pattern {} not found in mapping", pattern_match.pattern_id)));
                        }
                        
                        // Mark covered positions
                        for i in 0..pattern_match.length {
                            if pattern_match.position + i < covered_positions.len() {
                                covered_positions[pattern_match.position + i] = true;
                            }
                        }
                    }
                }
                
                // 4. Write terminator (position 0)
                if let Some(zero_entry) = pos_dict.get(&0) {
                    println!("  Writing terminator 0 -> code={:b} bits={}", zero_entry.code, zero_entry.code_bits);
                    bit_writer.write_bits(zero_entry.code, zero_entry.code_bits)?;
                } else {
                    return Err(Error::InvalidFormat("Terminator position 0 not in dictionary".into()));
                }
                
                // 5. Flush bit stream for this word (aligns to byte boundary)
                bit_writer.flush()?;
                file.write_all(&bit_buffer)?;
                
                // 6. Write uncovered bytes for this word
                let mut uncovered_count = 0;
                for (i, &byte) in word.iter().enumerate() {
                    if i >= covered_positions.len() || !covered_positions[i] {
                        file.write_all(&[byte])?;
                        uncovered_count += 1;
                    }
                }
                
                println!("  Uncovered bytes: {} out of {}", uncovered_count, word.len());
            }
            
        } else {
            // No position dictionary - use varints (simpler case)
            let mut buf = Vec::new();
            for _ in 0..self.words_count {
                let len_encoded = read_varint(&mut reader)?;
                let _is_compressed = (len_encoded & 1) == 0;
                let len = (len_encoded >> 1) as usize;
                
                buf.clear();
                buf.resize(len, 0);
                if len > 0 {
                    reader.read_exact(&mut buf)?;
                }
                
                let position = (len + 1) as u64;
                write_varint(file, position)?;
                file.write_all(&buf)?;
            }
        }
        
        Ok(())
    }

    fn write_position_bits(&self, writer: &mut impl Write, pos: u64) -> Result<()> {
        // When there's no position dictionary, positions are written as varints
        write_varint(writer, pos)?;
        Ok(())
    }

    fn rebuild_huffman_codes(&self, patterns: &mut [Pattern]) -> Result<()> {
        println!("Rebuilding Huffman codes with actual usage statistics...");
        
        // Print usage statistics
        for (i, pattern) in patterns.iter().enumerate().take(5) {
            println!("  Pattern {}: uses={}, data={:?}", i, pattern.uses, String::from_utf8_lossy(&pattern.word));
        }
        
        // Build proper Huffman codes based on actual usage
        if !patterns.is_empty() {
            self.assign_pattern_huffman_codes(patterns)?;
        }
        
        Ok(())
    }
    
    fn collect_pattern_positions(&mut self, patterns: &mut [Pattern]) -> Result<()> {
        println!("Collecting pattern positions and usage for dictionary building...");
        
        // Re-open uncompressed file for reading to collect positions
        self.uncompressed_file.flush()?;
        let uncompressed_file = File::open(&self.uncompressed_file.file_path)?;
        let mut reader = BufReader::new(uncompressed_file);
        
        for word_idx in 0..self.words_count {
            let len_encoded = read_varint(&mut reader)?;
            let _is_compressed = (len_encoded & 1) == 0;
            let len = (len_encoded >> 1) as usize;
            
            let mut word = vec![0u8; len];
            if len > 0 {
                reader.read_exact(&mut word)?;
            }
            
            // Find pattern matches for this word
            let pattern_matches = self.cover_word_with_patterns(&word, patterns);
            
            // Track pattern usage
            for pattern_match in &pattern_matches {
                if pattern_match.pattern_id < patterns.len() {
                    patterns[pattern_match.pattern_id].uses += 1;
                }
            }
            
            // Track relative positions that would be used
            let mut last_pos = 0u64;
            for pattern_match in &pattern_matches {
                let relative_pos = (pattern_match.position as u64 + 1) - last_pos;
                last_pos = pattern_match.position as u64 + 1;
                
                // Add this relative position to the position counts
                *self.position_counts.entry(relative_pos).or_insert(0) += 1;
            }
        }
        
        println!("Collected positions: {:?}", self.position_counts.keys().collect::<Vec<_>>());
        Ok(())
    }

    pub fn close(self) -> Result<()> {
        self.uncompressed_file.close_and_remove()?;
        Ok(())
    }
}

// Helper functions
fn write_varint(writer: &mut impl Write, mut value: u64) -> Result<()> {
    loop {
        if value < 0x80 {
            writer.write_all(&[value as u8])?;
            return Ok(());
        }
        writer.write_all(&[((value & 0x7F) | 0x80) as u8])?;
        value >>= 7;
    }
}

fn write_varint_to_vec(vec: &mut Vec<u8>, mut value: u64) -> Result<()> {
    loop {
        if value < 0x80 {
            vec.push(value as u8);
            return Ok(());
        }
        vec.push(((value & 0x7F) | 0x80) as u8);
        value >>= 7;
    }
}

fn read_varint(reader: &mut impl Read) -> Result<u64> {
    let mut result = 0u64;
    let mut shift = 0;
    let mut buf = [0u8; 1];
    
    loop {
        reader.read_exact(&mut buf)?;
        let byte = buf[0];
        
        if shift == 63 && byte > 1 {
            return Err(Error::InvalidFormat("Varint too large".into()));
        }
        
        result |= ((byte & 0x7F) as u64) << shift;
        
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        
        shift += 7;
    }
}

