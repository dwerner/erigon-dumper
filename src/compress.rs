use crate::error::{Result, Error};
use std::path::{Path, PathBuf};
use std::fs::File;
use std::io::{Write, BufWriter, BufReader, Read};
use std::collections::{HashMap, BinaryHeap};
use std::cmp::Ordering;

// Configuration matching Go's Cfg struct
pub struct CompressorCfg {
    pub min_pattern_score: u64,
    pub min_pattern_len: usize,
    pub max_pattern_len: usize,
    pub sampling_factor: u64,
    pub max_dict_patterns: usize,
    pub dict_reducer_soft_limit: usize,
    pub workers: usize,
}

impl Default for CompressorCfg {
    fn default() -> Self {
        // Match Go's DefaultCfg
        CompressorCfg {
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

/// Pattern represents a byte sequence found in data
#[derive(Clone, Debug)]
struct Pattern {
    data: Vec<u8>,
    score: u64,
    depth: u64,
}

/// PatternDict holds patterns for dictionary compression
struct PatternDict {
    patterns: Vec<Pattern>,
}

impl PatternDict {
    fn new() -> Self {
        PatternDict {
            patterns: Vec::new(),
        }
    }
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
    cfg: CompressorCfg,
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

impl Compressor {
    pub fn new(
        _log_prefix: &str,
        output_file: &Path,
        tmp_dir: &Path,
        cfg: CompressorCfg,
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
        
        // Build dictionary from patterns
        let dict = self.build_dictionary()?;
        
        // Write compressed file
        self.write_compressed_file(&dict)?;
        
        // Rename tmp file to final file
        std::fs::rename(&self.tmp_out_file_path, &self.output_file)?;
        
        Ok(())
    }

    fn build_dictionary(&self) -> Result<PatternDict> {
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
                        data: normal_pattern,
                        score,
                        depth: 0, // Will be assigned during Huffman tree building
                    });
                }
            }
        }
        
        println!("Selected {} patterns before sorting and truncation", patterns.len());
        
        // Sort by score descending
        patterns.sort_by(|a, b| b.score.cmp(&a.score));
        
        // Limit to max_dict_patterns, but also impose a reasonable limit
        let max_patterns = std::cmp::min(self.cfg.max_dict_patterns, 1000);
        patterns.truncate(max_patterns);
        
        println!("Final dictionary has {} patterns after truncation (max={})", 
                 patterns.len(), self.cfg.max_dict_patterns);
        
        // For simple implementation, assign depths with very reasonable limits
        // (Real implementation would build Huffman tree)
        // Keep depths very small to avoid deep recursion
        const MAX_PATTERN_DEPTH: u64 = 10; // Much smaller for safety
        for (i, pattern) in patterns.iter_mut().enumerate() {
            pattern.depth = ((i % MAX_PATTERN_DEPTH as usize) + 1) as u64;
        }
        
        Ok(PatternDict { patterns })
    }

    fn write_compressed_file(&mut self, dict: &PatternDict) -> Result<()> {
        let mut file = BufWriter::new(File::create(&self.tmp_out_file_path)?);
        
        // Header: 24 bytes
        // - words_count (8 bytes, big-endian)
        file.write_all(&self.words_count.to_be_bytes())?;
        file.write_all(&self.empty_words_count.to_be_bytes())?;
        
        // Write pattern dictionary
        let dict_data = self.encode_pattern_dict(dict)?;
        file.write_all(&(dict_data.len() as u64).to_be_bytes())?;
        file.write_all(&dict_data)?;
        
        // Write position dictionary
        let pos_dict_data = self.encode_pos_dict(dict)?;
        file.write_all(&(pos_dict_data.len() as u64).to_be_bytes())?;
        file.write_all(&pos_dict_data)?;
        
        // Build position lookup for compression
        let pos_dict = self.build_pos_lookup()?;
        
        // Write compressed words
        self.write_compressed_words(&mut file, dict, &pos_dict)?;
        
        file.flush()?;
        Ok(())
    }

    fn encode_pattern_dict(&self, dict: &PatternDict) -> Result<Vec<u8>> {
        let mut data = Vec::new();
        
        println!("Encoding pattern dictionary with {} patterns", dict.patterns.len());
        
        // Write each pattern as: varint(depth), varint(len), pattern_bytes
        for (i, pattern) in dict.patterns.iter().enumerate() {
            if i < 5 {
                println!("  Pattern {}: depth={}, len={}, data={:?}", 
                         i, pattern.depth, pattern.data.len(), 
                         String::from_utf8_lossy(&pattern.data));
            }
            write_varint_to_vec(&mut data, pattern.depth)?;
            write_varint_to_vec(&mut data, pattern.data.len() as u64)?;
            data.extend_from_slice(&pattern.data);
        }
        
        println!("Pattern dictionary encoded: {} bytes", data.len());
        Ok(data)
    }

    fn encode_pos_dict(&self, _dict: &PatternDict) -> Result<Vec<u8>> {
        if self.position_counts.is_empty() {
            return Ok(Vec::new());
        }
        
        // Build Huffman tree for positions
        let positions: Vec<Position> = self.position_counts.iter()
            .map(|(&pos, &uses)| Position {
                pos,
                uses,
                code: 0,
                code_bits: 0,
                depth: 0,
            })
            .collect();
        
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

    fn write_compressed_words(&mut self, file: &mut BufWriter<File>, dict: &PatternDict, pos_dict: &HashMap<u64, Position>) -> Result<()> {
        println!("Writing {} compressed words, dict has {} patterns", self.words_count, dict.patterns.len());
        println!("Position dictionary has {} entries", pos_dict.len());
        
        // Re-open uncompressed file for reading
        self.uncompressed_file.flush()?;
        let uncompressed_file = File::open(&self.uncompressed_file.file_path)?;
        let mut reader = BufReader::new(uncompressed_file);
        
        if !pos_dict.is_empty() {
            // Use bit encoding for positions
            let mut output_buffer = Vec::new();
            let mut bit_writer = BitWriter::new(&mut output_buffer);
            
            // First pass: write all positions
            let mut words_data = Vec::new();
            for _ in 0..self.words_count {
                let len_encoded = read_varint(&mut reader)?;
                let _is_compressed = (len_encoded & 1) == 0;
                let len = (len_encoded >> 1) as usize;
                
                let mut word = vec![0u8; len];
                if len > 0 {
                    reader.read_exact(&mut word)?;
                }
                
                let position = (len + 1) as u64;
                println!("Encoding position {} for word of len {}", position, len);
                
                // Write word length + 1 as position
                if let Some(pos_entry) = pos_dict.get(&position) {
                    println!("  Position {} -> code={:b} bits={}", position, pos_entry.code, pos_entry.code_bits);
                    bit_writer.write_bits(pos_entry.code, pos_entry.code_bits)?;
                } else {
                    return Err(Error::InvalidFormat(format!("Position {} not in dictionary", position)));
                }
                
                // Write terminator (position 0)
                // In the Go code, position 0 means end of positions for this word
                if dict.patterns.is_empty() {
                    // No patterns, so we need to write 0 position to indicate end
                    if let Some(zero_entry) = pos_dict.get(&0) {
                        println!("  Terminator 0 -> code={:b} bits={}", zero_entry.code, zero_entry.code_bits);
                        bit_writer.write_bits(zero_entry.code, zero_entry.code_bits)?;
                    } else {
                        // If 0 is not in dictionary, we have a problem
                        // Let's add it to our position counts
                        println!("Warning: No zero terminator in position dictionary");
                    }
                }
                
                words_data.push(word);
            }
            
            // Flush bit writer
            bit_writer.flush()?;
            
            // Write bit-encoded positions
            file.write_all(&output_buffer)?;
            
            // Write word data
            for word in words_data {
                file.write_all(&word)?;
            }
        } else {
            // No position dictionary - use varints
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

