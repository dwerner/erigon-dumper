// Port of Erigon's parallel_compress.go
// Original: go/src/parallel_compress.go

use crate::compress::{CompressionWord, Pattern, PatternHuff, Position, PositionHuff, Ring};
use crate::error::CompressionError;
use radix_trie::Trie;

// From Go: CompressionQueue type
// Go: parallel_compress.go:211
pub type CompressionQueue = Vec<CompressionWord>;

// Helper struct for pattern matching
// Replaces patricia.MatchFinder2 from Go
pub struct MatchFinder {
    trie: Trie<Vec<u8>, Box<Pattern>>, // Maps pattern bytes to Pattern objects
    patterns: Vec<Box<Pattern>>,       // Keep patterns alive
}

impl MatchFinder {
    pub fn new() -> Self {
        MatchFinder {
            trie: Trie::new(),
            patterns: Vec::new(),
        }
    }

    pub fn insert(&mut self, pattern: Pattern) {
        let pattern_box = Box::new(pattern);
        let pattern_ptr = pattern_box.clone();
        self.trie.insert(pattern_box.word.clone(), pattern_ptr);
        self.patterns.push(pattern_box);
    }

    // Find all patterns that match starting at any position in input
    // This is equivalent to Go's FindLongestMatches
    pub fn find_longest_matches(&self, input: &[u8]) -> Vec<Match> {
        let mut matches = Vec::new();

        // For each starting position in input
        for start in 0..input.len() {
            // Check all possible pattern lengths from this position
            for end in (start + 1)..=input.len().min(start + 128) {
                // Max pattern len
                if let Some(pattern) = self.trie.get(&input[start..end]) {
                    matches.push(Match {
                        pattern: pattern.clone(),
                        start,
                        end,
                    });
                }
            }
        }

        // Sort matches by start position, then by length (longest first)
        matches.sort_by(|a, b| a.start.cmp(&b.start).then_with(|| b.end.cmp(&a.end)));

        // Remove overlapping shorter matches
        let mut filtered = Vec::new();
        let mut last_end = 0;

        for m in matches {
            if m.start >= last_end {
                last_end = m.end;
                filtered.push(m);
            }
        }

        filtered
    }
}

// Equivalent to Go's Match struct
pub struct Match {
    pub pattern: Box<Pattern>, // The pattern that matched
    pub start: usize,          // Start position in input
    pub end: usize,            // End position in input
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
    pos_map: &mut std::collections::HashMap<u64, u64>,
) -> (Vec<u8>, Vec<usize>, Vec<usize>) {
    // Go: parallel_compress.go:42-179

    // Clear output buffer
    output.clear();

    // Find all pattern matches in the input
    let matches = match_finder.find_longest_matches(input);

    // Go: parallel_compress.go:45-48
    if matches.is_empty() {
        // No patterns found - encode as uncompressed
        output.push(0); // Encoding of 0 in VarUint is 1 zero byte
        output.extend_from_slice(input);
        return (output.clone(), patterns.clone(), uncovered.clone());
    }

    if trace {
        println!("Cluster | input = {:?}", input);
        for match_info in &matches {
            println!(
                " [{:?} {}-{}]",
                &input[match_info.start..match_info.end],
                match_info.start,
                match_info.end
            );
        }
    }

    // Go: parallel_compress.go:56-66
    // Initialize dynamic programming ring buffer
    cell_ring.reset();
    patterns.clear();
    patterns.push(0); // Sentinel entry
    patterns.push(0);

    // Initialize cells for the last match
    if let Some(last_match) = matches.last() {
        for j in last_match.start..last_match.end {
            let d = cell_ring.push_back();
            d.optim_start = j + 1;
            d.cover_start = input.len();
            d.compression = 0;
            d.pattern_idx = 0;
            d.score = 0;
        }
    }

    // Go: parallel_compress.go:68-128
    // Dynamic programming to find optimal pattern coverage
    for i in (0..matches.len()).rev() {
        let f = &matches[i];
        let first_cell = cell_ring.get(0);
        let mut max_compression = first_cell.compression;
        let mut max_score = first_cell.score;
        let mut max_cell_idx = 0;
        let mut max_include = false;

        for e in 0..cell_ring.len() {
            let cell = cell_ring.get(e);
            let mut comp = cell.compression as i32 - 4; // Cost of encoding pattern

            if cell.cover_start >= f.end {
                comp += (f.end - f.start) as i32;
            } else {
                comp += (cell.cover_start - f.start) as i32;
            }

            let score = cell.score + f.pattern.score;

            if comp > max_compression || (comp == max_compression && score > max_score) {
                max_compression = comp;
                max_score = score;
                max_include = true;
                max_cell_idx = e; // Store the index, not a reference
            } else if cell.optim_start > f.end {
                cell_ring.truncate(e);
                break;
            }
        }

        // Push front first
        cell_ring.push_front();

        // After push_front, all indices shift by 1
        let adjusted_idx = max_cell_idx + 1;

        // Get the values we need from max_cell
        let max_cell_pattern_idx = cell_ring.get(adjusted_idx).pattern_idx;
        let max_cell_cover_start = cell_ring.get(adjusted_idx).cover_start;

        // Now mutate the new front cell (index 0)
        let d = cell_ring.get(0);
        d.optim_start = f.start;
        d.score = max_score;
        d.compression = max_compression;

        if max_include {
            d.cover_start = f.start;
            d.pattern_idx = patterns.len();
            patterns.push(i);
            patterns.push(max_cell_pattern_idx);
        } else {
            d.cover_start = max_cell_cover_start;
            d.pattern_idx = max_cell_pattern_idx;
        }
    }

    // Go: parallel_compress.go:129-178
    // Build output from optimal solution
    let optim_cell = cell_ring.get(0);

    // Count number of patterns
    let mut pattern_count = 0u64;
    let mut pattern_idx = optim_cell.pattern_idx;
    while pattern_idx != 0 {
        pattern_count += 1;
        pattern_idx = patterns[pattern_idx + 1];
    }

    // Write pattern count
    let mut num_buf = [0u8; 10];
    let n = encode_varint(&mut num_buf, pattern_count);
    output.extend_from_slice(&num_buf[..n]);

    // Write patterns and track uncovered regions
    pattern_idx = optim_cell.pattern_idx;
    let mut last_start = 0;
    let mut last_uncovered = 0;
    uncovered.clear();

    while pattern_idx != 0 {
        let pattern_match_idx = patterns[pattern_idx];
        let pattern_match = &matches[pattern_match_idx];

        if pattern_match.start > last_uncovered {
            uncovered.push(last_uncovered);
            uncovered.push(pattern_match.start);
        }
        last_uncovered = pattern_match.end;

        // Starting position
        *pos_map
            .entry((pattern_match.start - last_start + 1) as u64)
            .or_insert(0) += 1;
        last_start = pattern_match.start;

        // Write position
        let n = encode_varint(&mut num_buf, pattern_match.start as u64);
        output.extend_from_slice(&num_buf[..n]);

        // Write pattern code
        let n = encode_varint(&mut num_buf, pattern_match.pattern.code);
        output.extend_from_slice(&num_buf[..n]);

        pattern_idx = patterns[pattern_idx + 1];
    }

    if input.len() > last_uncovered {
        uncovered.push(last_uncovered);
        uncovered.push(input.len());
    }

    // Add uncoded input
    for i in (0..uncovered.len()).step_by(2) {
        output.extend_from_slice(&input[uncovered[i]..uncovered[i + 1]]);
    }

    (output.clone(), patterns.clone(), uncovered.clone())
}

// From Go: Huffman tree building for patterns
// Go: parallel_compress.go:433-524
pub struct PatternHuffBuilder {
    pub patterns: Vec<Pattern>,
}

impl PatternHuffBuilder {
    pub fn new(patterns: Vec<Pattern>) -> Self {
        PatternHuffBuilder { patterns }
    }

    pub fn build_huffman_codes(&mut self) {
        // Go: parallel_compress.go:454-524
        // Build Huffman tree and assign codes to patterns
        use std::cmp::Ordering;
        use std::collections::BinaryHeap;

        // Sort patterns by uses (frequency) - least used first
        self.patterns.sort_by(|a, b| a.uses.cmp(&b.uses));

        if self.patterns.is_empty() {
            return;
        }

        // Wrapper for heap ordering
        struct HuffNode {
            node: Box<PatternHuff>,
        }

        impl Ord for HuffNode {
            fn cmp(&self, other: &Self) -> Ordering {
                // Reverse order for min-heap
                other
                    .node
                    .uses
                    .cmp(&self.node.uses)
                    .then_with(|| other.node.tie_breaker.cmp(&self.node.tie_breaker))
            }
        }

        impl PartialOrd for HuffNode {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        impl Eq for HuffNode {}
        impl PartialEq for HuffNode {
            fn eq(&self, other: &Self) -> bool {
                self.node.uses == other.node.uses && self.node.tie_breaker == other.node.tie_breaker
            }
        }

        let mut heap: BinaryHeap<HuffNode> = BinaryHeap::new();
        let mut i = 0;
        let mut tie_breaker = 0u64;

        // Build Huffman tree
        while heap.len() + (self.patterns.len() - i) > 1 {
            let mut h = PatternHuff {
                p0: None,
                p1: None,
                h0: None,
                h1: None,
                uses: 0,
                tie_breaker,
            };

            // Take first child (0 bit)
            if !heap.is_empty()
                && (i >= self.patterns.len()
                    || heap.peek().unwrap().node.uses < self.patterns[i].uses)
            {
                // Take from heap
                let mut node = heap.pop().unwrap();
                node.node.add_zero();
                h.uses += node.node.uses;
                h.h0 = Some(node.node);
            } else {
                // Take from list
                self.patterns[i].code = 0;
                self.patterns[i].code_bits = 1;
                h.uses += self.patterns[i].uses;
                h.p0 = Some(Box::new(self.patterns[i].clone()));
                i += 1;
            }

            // Take second child (1 bit)
            if !heap.is_empty()
                && (i >= self.patterns.len()
                    || heap.peek().unwrap().node.uses < self.patterns[i].uses)
            {
                // Take from heap
                let mut node = heap.pop().unwrap();
                node.node.add_one();
                h.uses += node.node.uses;
                h.h1 = Some(node.node);
            } else {
                // Take from list
                self.patterns[i].code = 1;
                self.patterns[i].code_bits = 1;
                h.uses += self.patterns[i].uses;
                h.p1 = Some(Box::new(self.patterns[i].clone()));
                i += 1;
            }

            tie_breaker += 1;
            heap.push(HuffNode { node: Box::new(h) });
        }

        // Set depths from root
        if let Some(mut root) = heap.pop() {
            root.node.set_depth(0);
            // Extract patterns back with their assigned codes
            self.extract_patterns(&*root.node);
        }
    }

    fn extract_patterns(&mut self, node: &PatternHuff) {
        // Recursively extract patterns from the Huffman tree
        // and update the patterns vector with their codes
        if let Some(ref p0) = node.p0 {
            // Find and update the pattern in our list
            for pattern in &mut self.patterns {
                if pattern.word == p0.word {
                    pattern.code = p0.code;
                    pattern.code_bits = p0.code_bits;
                    pattern.depth = p0.depth;
                    break;
                }
            }
        }
        if let Some(ref p1) = node.p1 {
            for pattern in &mut self.patterns {
                if pattern.word == p1.word {
                    pattern.code = p1.code;
                    pattern.code_bits = p1.code_bits;
                    pattern.depth = p1.depth;
                    break;
                }
            }
        }
        if let Some(ref h0) = node.h0 {
            self.extract_patterns(h0);
        }
        if let Some(ref h1) = node.h1 {
            self.extract_patterns(h1);
        }
    }
}

// From Go: Huffman tree building for positions
// Go: parallel_compress.go:533-625
pub struct PositionHuffBuilder {
    pub positions: Vec<Position>,
}

impl PositionHuffBuilder {
    pub fn new(positions: Vec<Position>) -> Self {
        PositionHuffBuilder { positions }
    }

    pub fn build_huffman_codes(&mut self) {
        // Go: parallel_compress.go:554-625
        // Build Huffman tree and assign codes to positions
        use std::cmp::Ordering;
        use std::collections::BinaryHeap;

        // Sort positions by uses (frequency) - least used first
        self.positions.sort_by(|a, b| a.uses.cmp(&b.uses));

        if self.positions.is_empty() {
            return;
        }

        // Wrapper for heap ordering
        struct HuffNode {
            node: Box<PositionHuff>,
        }

        impl Ord for HuffNode {
            fn cmp(&self, other: &Self) -> Ordering {
                // Reverse order for min-heap
                other
                    .node
                    .uses
                    .cmp(&self.node.uses)
                    .then_with(|| other.node.tie_breaker.cmp(&self.node.tie_breaker))
            }
        }

        impl PartialOrd for HuffNode {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        impl Eq for HuffNode {}
        impl PartialEq for HuffNode {
            fn eq(&self, other: &Self) -> bool {
                self.node.uses == other.node.uses && self.node.tie_breaker == other.node.tie_breaker
            }
        }

        let mut heap: BinaryHeap<HuffNode> = BinaryHeap::new();
        let mut i = 0;
        let mut tie_breaker = 0u64;

        // Build Huffman tree
        while heap.len() + (self.positions.len() - i) > 1 {
            let mut h = PositionHuff {
                p0: None,
                p1: None,
                h0: None,
                h1: None,
                uses: 0,
                tie_breaker,
            };

            // Take first child (0 bit)
            if !heap.is_empty()
                && (i >= self.positions.len()
                    || heap.peek().unwrap().node.uses < self.positions[i].uses)
            {
                // Take from heap
                let mut node = heap.pop().unwrap();
                node.node.add_zero();
                h.uses += node.node.uses;
                h.h0 = Some(node.node);
            } else {
                // Take from list
                self.positions[i].code = 0;
                self.positions[i].code_bits = 1;
                h.uses += self.positions[i].uses;
                h.p0 = Some(Box::new(self.positions[i].clone()));
                i += 1;
            }

            // Take second child (1 bit)
            if !heap.is_empty()
                && (i >= self.positions.len()
                    || heap.peek().unwrap().node.uses < self.positions[i].uses)
            {
                // Take from heap
                let mut node = heap.pop().unwrap();
                node.node.add_one();
                h.uses += node.node.uses;
                h.h1 = Some(node.node);
            } else {
                // Take from list
                self.positions[i].code = 1;
                self.positions[i].code_bits = 1;
                h.uses += self.positions[i].uses;
                h.p1 = Some(Box::new(self.positions[i].clone()));
                i += 1;
            }

            tie_breaker += 1;
            heap.push(HuffNode { node: Box::new(h) });
        }

        // Set depths from root
        if let Some(mut root) = heap.pop() {
            root.node.set_depth(0);
            // Extract positions back with their assigned codes
            self.extract_positions(&*root.node);
        }
    }

    fn extract_positions(&mut self, node: &PositionHuff) {
        // Recursively extract positions from the Huffman tree
        // and update the positions vector with their codes
        if let Some(ref p0) = node.p0 {
            // Find and update the position in our list
            for position in &mut self.positions {
                if position.pos == p0.pos {
                    position.code = p0.code;
                    position.code_bits = p0.code_bits;
                    position.depth = p0.depth;
                    break;
                }
            }
        }
        if let Some(ref p1) = node.p1 {
            for position in &mut self.positions {
                if position.pos == p1.pos {
                    position.code = p1.code;
                    position.code_bits = p1.code_bits;
                    position.depth = p1.depth;
                    break;
                }
            }
        }
        if let Some(ref h0) = node.h0 {
            self.extract_positions(h0);
        }
        if let Some(ref h1) = node.h1 {
            self.extract_positions(h1);
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
    use std::fs::File;
    use std::io::{BufWriter, Write};

    // Go: parallel_compress.go:243-255
    // Build pattern dictionary and trie
    let mut match_finder = MatchFinder::new();
    let mut code2pattern = Vec::with_capacity(256);

    dict_builder.for_each(|score, word| {
        let mut pattern = Pattern::new(word.to_vec(), score);
        pattern.code = code2pattern.len() as u64;
        pattern.uses = 0;
        pattern.code_bits = 0;

        match_finder.insert(pattern.clone());
        code2pattern.push(pattern);
    });

    if cfg.workers > 1 {
        // Multi-worker mode not yet implemented
        log::warn!(
            "[{}] Multi-worker compression not yet implemented, using single worker",
            log_prefix
        );
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
                    &mut uncomp_pos_map,
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

    log::debug!(
        "[{}] Intermediate file written, processing {} words",
        log_prefix,
        out_count
    );

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

// Helper to decode varint
fn decode_varint(data: &[u8]) -> Result<(u64, usize), CompressionError> {
    let mut value = 0u64;
    let mut shift = 0;

    for (i, &byte) in data.iter().enumerate() {
        if i == 10 {
            return Err(CompressionError::Other("Varint too long".to_string()));
        }

        value |= ((byte & 0x7F) as u64) << shift;

        if byte & 0x80 == 0 {
            return Ok((value, i + 1));
        }

        shift += 7;
    }

    Err(CompressionError::Other(
        "Unexpected end of varint".to_string(),
    ))
}

// BitWriter for Huffman encoding
// From Go: compress.go:636
struct BitWriter<W: std::io::Write> {
    w: W,
    output_bits: usize,
    output_byte: u8,
}

impl<W: std::io::Write> BitWriter<W> {
    fn new(w: W) -> Self {
        BitWriter {
            w,
            output_bits: 0,
            output_byte: 0,
        }
    }

    fn encode(&mut self, mut code: u64, mut code_bits: usize) -> std::io::Result<()> {
        while code_bits > 0 {
            let bits_used = if self.output_bits + code_bits > 8 {
                8 - self.output_bits
            } else {
                code_bits
            };

            let mask = (1u64 << bits_used) - 1;
            self.output_byte |= ((code & mask) << self.output_bits) as u8;
            code >>= bits_used;
            code_bits -= bits_used;
            self.output_bits += bits_used;

            if self.output_bits == 8 {
                self.w.write_all(&[self.output_byte])?;
                self.output_bits = 0;
                self.output_byte = 0;
            }
        }
        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if self.output_bits > 0 {
            self.w.write_all(&[self.output_byte])?;
            self.output_bits = 0;
            self.output_byte = 0;
        }
        self.w.flush()
    }

    // Helper to write raw bytes (flushes bits first)
    fn write_bytes(&mut self, data: &[u8]) -> std::io::Result<()> {
        self.flush()?;
        self.w.write_all(data)
    }

    // Get the inner writer (consumes self)
    fn into_inner(mut self) -> std::io::Result<W> {
        self.flush()?;
        Ok(self.w)
    }
}

// Write the final compressed file with Huffman tables
fn write_compressed_file(
    cf: &mut std::fs::File,
    intermediate_path: &str,
    patterns: &[Pattern],
    positions: &[Position],
) -> std::result::Result<(), CompressionError> {
    use std::collections::HashMap;
    use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};

    let mut w = BufWriter::new(cf);

    // First pass: count words and collect statistics
    let mut intermediate = std::fs::File::open(intermediate_path)?;
    let mut word_count = 0u64;
    let mut empty_word_count = 0u64;

    loop {
        let mut len_buf = [0u8; 10];
        let mut bytes_read = 0;

        // Try to read varint length
        while bytes_read < len_buf.len() {
            let n = intermediate.read(&mut len_buf[bytes_read..bytes_read + 1])?;
            if n == 0 {
                break;
            }
            bytes_read += 1;
            if len_buf[bytes_read - 1] & 0x80 == 0 {
                break;
            }
        }

        if bytes_read == 0 {
            break; // EOF
        }

        let (word_len, _) = decode_varint(&len_buf[..bytes_read])?;
        word_count += 1;

        if word_len == 0 {
            empty_word_count += 1;
        } else {
            // Skip the word data
            let mut skip_buf = vec![0u8; word_len as usize];
            intermediate.read_exact(&mut skip_buf)?;
        }
    }

    // Write header
    w.write_all(&word_count.to_be_bytes())?;
    w.write_all(&empty_word_count.to_be_bytes())?;

    // Write pattern dictionary
    let mut pattern_dict_data = Vec::new();
    let mut varint_buf = [0u8; 10];

    for pattern in patterns {
        let n = encode_varint(&mut varint_buf, pattern.depth as u64);
        pattern_dict_data.extend_from_slice(&varint_buf[..n]);
        let n = encode_varint(&mut varint_buf, pattern.word.len() as u64);
        pattern_dict_data.extend_from_slice(&varint_buf[..n]);
        pattern_dict_data.extend_from_slice(&pattern.word);
    }

    w.write_all(&(pattern_dict_data.len() as u64).to_be_bytes())?;
    w.write_all(&pattern_dict_data)?;

    // Write position dictionary
    let mut pos_dict_data = Vec::new();

    for position in positions {
        let n = encode_varint(&mut varint_buf, position.depth as u64);
        pos_dict_data.extend_from_slice(&varint_buf[..n]);
        let n = encode_varint(&mut varint_buf, position.pos);
        pos_dict_data.extend_from_slice(&varint_buf[..n]);
    }

    w.write_all(&(pos_dict_data.len() as u64).to_be_bytes())?;
    w.write_all(&pos_dict_data)?;

    // Build lookup maps for codes
    let mut code2pattern: HashMap<u64, &Pattern> = HashMap::new();
    for pattern in patterns {
        code2pattern.insert(pattern.code, pattern);
    }

    let mut pos2code: HashMap<u64, &Position> = HashMap::new();
    for position in positions {
        pos2code.insert(position.pos, position);
    }

    // Second pass: re-encode with Huffman codes
    intermediate.seek(SeekFrom::Start(0))?;
    let mut reader = BufReader::new(intermediate);
    let mut bit_writer = BitWriter::new(&mut w);

    loop {
        // Read word length
        let mut len_buf = [0u8; 10];
        let mut bytes_read = 0;

        while bytes_read < len_buf.len() {
            let n = reader.read(&mut len_buf[bytes_read..bytes_read + 1])?;
            if n == 0 {
                break;
            }
            bytes_read += 1;
            if len_buf[bytes_read - 1] & 0x80 == 0 {
                break;
            }
        }

        if bytes_read == 0 {
            break; // EOF
        }

        let (word_len, _) = decode_varint(&len_buf[..bytes_read])?;

        // Encode word length+1 with position huffman code
        if let Some(pos_code) = pos2code.get(&(word_len + 1)) {
            bit_writer.encode(pos_code.code, pos_code.code_bits)?;
        } else {
            // No huffman code - write varint directly
            let n = encode_varint(&mut varint_buf, word_len + 1);
            bit_writer.write_bytes(&varint_buf[..n])?;
        }

        if word_len == 0 {
            // Empty word
            bit_writer.flush()?;
        } else {
            // Read pattern count
            let mut pattern_count_buf = [0u8; 10];
            let mut bytes_read = 0;

            while bytes_read < pattern_count_buf.len() {
                let n = reader.read(&mut pattern_count_buf[bytes_read..bytes_read + 1])?;
                if n == 0 {
                    break;
                }
                bytes_read += 1;
                if pattern_count_buf[bytes_read - 1] & 0x80 == 0 {
                    break;
                }
            }

            let (pattern_count, _) = decode_varint(&pattern_count_buf[..bytes_read])?;

            if pattern_count == 0 {
                // No patterns - word is uncompressed
                let mut word_data = vec![0u8; word_len as usize];
                reader.read_exact(&mut word_data)?;
                bit_writer.write_bytes(&word_data)?;
            } else {
                // Process patterns
                let mut last_pos = 0u64;
                let mut uncovered_count = 0usize;
                let mut last_uncovered = 0usize;

                for _ in 0..pattern_count {
                    // Read pattern position
                    let mut pos_buf = [0u8; 10];
                    let mut bytes_read = 0;

                    while bytes_read < pos_buf.len() {
                        let n = reader.read(&mut pos_buf[bytes_read..bytes_read + 1])?;
                        if n == 0 {
                            break;
                        }
                        bytes_read += 1;
                        if pos_buf[bytes_read - 1] & 0x80 == 0 {
                            break;
                        }
                    }

                    let (pos, _) = decode_varint(&pos_buf[..bytes_read])?;

                    // Encode relative position with huffman code
                    if let Some(pos_code) = pos2code.get(&(pos - last_pos + 1)) {
                        bit_writer.encode(pos_code.code, pos_code.code_bits)?;
                    }
                    last_pos = pos;

                    // Read pattern code
                    let mut code_buf = [0u8; 10];
                    let mut bytes_read = 0;

                    while bytes_read < code_buf.len() {
                        let n = reader.read(&mut code_buf[bytes_read..bytes_read + 1])?;
                        if n == 0 {
                            break;
                        }
                        bytes_read += 1;
                        if code_buf[bytes_read - 1] & 0x80 == 0 {
                            break;
                        }
                    }

                    let (pattern_code, _) = decode_varint(&code_buf[..bytes_read])?;

                    // Encode pattern with huffman code
                    if let Some(pattern) = code2pattern.get(&pattern_code) {
                        bit_writer.encode(pattern.code, pattern.code_bits)?;

                        // Track uncovered bytes
                        if pos as usize > last_uncovered {
                            uncovered_count += pos as usize - last_uncovered;
                        }
                        last_uncovered = pos as usize + pattern.word.len();
                    }
                }

                // Calculate total uncovered bytes
                if word_len as usize > last_uncovered {
                    uncovered_count += word_len as usize - last_uncovered;
                }

                // Write terminating position code
                if let Some(pos_code) = pos2code.get(&0) {
                    bit_writer.encode(pos_code.code, pos_code.code_bits)?;
                }
                bit_writer.flush()?;

                // Copy uncovered bytes
                if uncovered_count > 0 {
                    let mut uncovered_data = vec![0u8; uncovered_count];
                    reader.read_exact(&mut uncovered_data)?;
                    bit_writer.write_bytes(&uncovered_data)?;
                }
            }
        }
    }

    // Finish with BitWriter and get back the underlying writer
    let mut w = bit_writer.into_inner()?;
    w.flush()?;

    log::debug!("Compressed file written successfully");
    Ok(())
}

// From Go: extractPatternsInSuperstrings function
// Go: parallel_compress.go:744
pub fn extract_patterns_in_superstrings(
    superstrings: Vec<Vec<u8>>,
    cfg: &crate::compress::Cfg,
) -> Vec<Pattern> {
    // Go: parallel_compress.go:744-824
    use cdivsufsort::sort_in_place;
    use std::collections::HashMap;

    let mut pattern_map: HashMap<Vec<u8>, u64> = HashMap::new();
    let min_pattern_len = cfg.min_pattern_len;
    let max_pattern_len = cfg.max_pattern_len;
    let min_pattern_score = cfg.min_pattern_score;

    for superstring in superstrings {
        if superstring.is_empty() {
            continue;
        }

        // Build suffix array using divsufsort
        // Go: parallel_compress.go:764
        let mut sa = vec![0i32; superstring.len()];
        sort_in_place(&superstring, &mut sa);

        // Filter out suffixes that start with odd positions
        // Go: parallel_compress.go:769-778
        let n = sa.len() / 2;
        let mut filtered = Vec::with_capacity(n);
        for &pos in &sa {
            if pos & 1 == 0 {
                filtered.push(pos >> 1); // Divide by 2 to get actual position
            }
        }

        // Build inverse suffix array
        // Go: parallel_compress.go:779-787
        let mut inv = vec![0i32; filtered.len()];
        for (i, &pos) in filtered.iter().enumerate() {
            if (pos as usize) < inv.len() {
                inv[pos as usize] = i as i32;
            }
        }

        // Compute LCP array using Kasai's algorithm
        // Go: parallel_compress.go:789-823
        let mut lcp = vec![0i32; filtered.len()];
        let mut k = 0;

        for i in 0..filtered.len() {
            if inv[i] == (filtered.len() - 1) as i32 {
                k = 0;
                lcp[inv[i] as usize] = 0;
                continue;
            }

            let j = filtered[(inv[i] + 1) as usize] as usize;
            let i_pos = i;

            // Compare characters at positions i+k and j+k
            // Go: parallel_compress.go:814
            while i_pos + k < filtered.len()
                && j + k < filtered.len()
                && i_pos * 2 + k * 2 < superstring.len()
                && j * 2 + k * 2 < superstring.len()
                && superstring[i_pos * 2 + k * 2] != 0
                && superstring[j * 2 + k * 2] != 0
                && superstring[i_pos * 2 + k * 2 + 1] == superstring[j * 2 + k * 2 + 1]
            {
                k += 1;
            }

            lcp[inv[i] as usize] = k as i32;

            if k > 0 {
                k -= 1;
            }
        }

        // Extract patterns based on LCP values
        // Patterns are substrings that appear multiple times (LCP > 0)
        // and meet length and score requirements
        for i in 0..lcp.len() {
            let prefix_len = lcp[i] as usize;

            if prefix_len >= min_pattern_len && prefix_len <= max_pattern_len {
                let pos = filtered[i] as usize;

                // Extract the pattern bytes from the superstring
                // Remember: superstring uses 2 bytes per character
                let mut pattern_bytes = Vec::with_capacity(prefix_len);
                for j in 0..prefix_len {
                    if pos * 2 + j * 2 + 1 < superstring.len() {
                        pattern_bytes.push(superstring[pos * 2 + j * 2 + 1]);
                    }
                }

                // Increment score for this pattern
                *pattern_map.entry(pattern_bytes).or_insert(0) += 1;
            }
        }
    }

    // Convert to Pattern objects and filter by score
    let mut patterns = Vec::new();
    for (word, score) in pattern_map {
        if score >= min_pattern_score {
            patterns.push(Pattern::new(word, score));
        }
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
        for pattern in patterns {
            trie.insert(pattern.clone());
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
        mf.insert(Pattern::new(b"hello".to_vec(), 100));
        mf.insert(Pattern::new(b"world".to_vec(), 200));

        let input = b"hello world";
        let matches = mf.find_longest_matches(input);
        assert!(!matches.is_empty());
        assert_eq!(matches[0].start, 0);
        assert_eq!(matches[0].end, 5);
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
