use crate::error::{Error, Result};
use std::path::Path;
use memmap2::{Mmap, MmapOptions};
use std::fs::File;

// Decompressor implementation matching Go's seg/decompress.go

const MAX_ALLOWED_DEPTH: u64 = 50;
const COMPRESSED_MIN_SIZE: usize = 32;
const CONDENSE_PATTERN_TABLE_BIT_THRESHOLD: usize = 9;

// Decompressor provides access to the superstrings in a file produced by a compressor
pub struct Decompressor<'a> {
    data: &'a [u8],                  // []byte
    dict: Option<PatternTable>,      // *patternTable
    pos_dict: Option<PosTable>,       // *posTable
    words_start: u64,                 // uint64
    words_count: u64,                 // uint64
    empty_words_count: u64,           // uint64
}

// DecompressorOwned wraps Decompressor with owned mmap data
pub struct DecompressorOwned {
    _mmap: Mmap,
    inner: Decompressor<'static>,
}

impl DecompressorOwned {
    pub fn open(path: &Path) -> Result<Self> {
        // println!("Opening decompressor for: {:?}", path);
        let file = File::open(path)?;
        let metadata = file.metadata()?;
        
        if metadata.len() < COMPRESSED_MIN_SIZE as u64 {
            return Err(Error::InvalidFormat(format!(
                "File too small: {} bytes, expected at least {}",
                metadata.len(),
                COMPRESSED_MIN_SIZE
            )));
        }

        let mmap = unsafe { MmapOptions::new().map(&file)? };
        
        // Create decompressor with a reference to the mmap data
        // This is safe because we store the mmap in the struct, ensuring it lives as long as the decompressor
        let data: &[u8] = &mmap[..];
        let data_static: &'static [u8] = unsafe { std::mem::transmute(data) };
        let inner = Decompressor::new(data_static)?;
        
        Ok(DecompressorOwned { _mmap: mmap, inner })
    }
    
    pub fn make_getter(&self) -> Getter {
        self.inner.make_getter()
    }
}

// word type alias matching Go's word []byte
type Word = Vec<u8>;

// codeword struct
struct CodeWord {
    pattern: Word,                    // word ([]byte)
    ptr: Option<Box<PatternTable>>,   // *patternTable
    code: u16,                        // uint16
    len: u8,                          // byte
}

// patternTable struct
struct PatternTable {
    patterns: Vec<Option<CodeWord>>,  // []*codeword
    bit_len: usize,                   // int
}

// posTable struct
struct PosTable {
    pos: Vec<u64>,                       // []uint64
    lens: Vec<u8>,                       // []byte
    ptrs: Vec<Option<Box<PosTable>>>,   // []*posTable
    bit_len: usize,                     // int
}

// Getter represents reader/iterator that can move across the data
pub struct Getter<'a> {
    pattern_dict: Option<&'a PatternTable>,  // *patternTable
    pos_dict: Option<&'a PosTable>,          // *posTable
    f_name: String,                          // string
    data: &'a [u8],                          // []byte
    pub data_p: u64,                         // uint64
    data_bit: usize,                         // int (0..7)
    trace: bool,                             // bool
}

impl<'a> Decompressor<'a> {
    pub fn new(data: &'a [u8]) -> Result<Self> {
        if data.len() < 24 {
            return Err(Error::InvalidFormat("Data too short for header".to_string()));
        }
        
        // Read header (Erigon uses big-endian)
        let words_count = u64::from_be_bytes(data[0..8].try_into().unwrap());
        let empty_words_count = u64::from_be_bytes(data[8..16].try_into().unwrap());
        let dict_size = u64::from_be_bytes(data[16..24].try_into().unwrap());
        
        println!("Decompressor header: words_count={}, empty_words_count={}, dict_size={}", 
                 words_count, empty_words_count, dict_size);
        
        if 24 + dict_size > data.len() as u64 {
            return Err(Error::InvalidFormat(format!(
                "Dictionary size {} exceeds file size",
                dict_size
            )));
        }

        // Parse pattern dictionary
        let mut pos = 24usize;
        let dict = if dict_size > 0 {
            let dict_data = &data[pos..pos + dict_size as usize];
            Some(Self::build_pattern_dict(dict_data)?)
        } else {
            None
        };
        pos += dict_size as usize;

        // Parse position dictionary
        let pos_dict_size = u64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        
        println!("Position dictionary size: {}", pos_dict_size);
        
        let pos_dict = if pos_dict_size > 0 {
            let dict_data = &data[pos..pos + pos_dict_size as usize];
            Some(Self::build_pos_dict(dict_data)?)
        } else {
            None
        };
        pos += pos_dict_size as usize;

        let words_start = pos;

        Ok(Decompressor {
            data,
            dict,
            pos_dict,
            words_start: words_start as u64,
            words_count,
            empty_words_count,
        })
    }

    fn build_pattern_dict(data: &[u8]) -> Result<PatternTable> {
        let mut depths = Vec::new();
        let mut patterns = Vec::new();
        let mut pos = 0;
        let mut max_depth = 0u64;

        // println!("Building pattern dict from {} bytes", data.len());

        // Parse patterns
        while pos < data.len() {
            let (depth, n) = decode_varint(&data[pos..])?;
            if depth > MAX_ALLOWED_DEPTH {
                return Err(Error::InvalidFormat(format!("Depth {} exceeds max", depth)));
            }
            if depth > max_depth {
                max_depth = depth;
            }
            depths.push(depth);
            pos += n;

            let (len, n) = decode_varint(&data[pos..])?;
            pos += n;
            
            if pos + len as usize > data.len() {
                return Err(Error::InvalidFormat("Pattern extends beyond data".into()));
            }
            
            patterns.push(data[pos..pos + len as usize].to_vec());
            pos += len as usize;
        }
        
        println!("Parsed {} patterns, max_depth={}", patterns.len(), max_depth);
        
        // Debug: print first few patterns
        for (i, (depth, pattern)) in depths.iter().zip(patterns.iter()).take(5).enumerate() {
            println!("  Pattern {}: depth={}, data={:?}", i, depth, String::from_utf8_lossy(pattern));
        }

        // Build pattern table
        let bit_len = if max_depth > 9 { 9 } else { max_depth as usize };
        let mut table = PatternTable::new(bit_len);
        
        Self::build_condensed_pattern_table(&mut table, &depths, &patterns, 0, 0, 0, max_depth)?;
        
        Ok(table)
    }

    fn build_condensed_pattern_table(
        table: &mut PatternTable,
        depths: &[u64],
        patterns: &[Vec<u8>],
        code: u16,
        bits: usize,
        depth: u64,
        max_depth: u64,
    ) -> Result<usize> {
        if depths.is_empty() {
            return Ok(0);
        }

        // Check if we've found a pattern to insert
        if depth == depths[0] {
            let cw = CodeWord {
                pattern: patterns[0].clone(),
                code,
                len: bits as u8,
                ptr: None,
            };
            println!("    Building pattern table: inserting pattern at depth={}, code={}, bits={}, pattern={:?}, table.bit_len={}", 
                     depth, code, bits, String::from_utf8_lossy(&patterns[0]), table.bit_len);
            table.insert_word(cw);
            return Ok(1);
        }

        if bits == 9 {
            let bit_len = if max_depth > 9 { 9 } else { max_depth as usize };
            let mut new_table = Box::new(PatternTable::new(bit_len));
            println!("    Creating deeper table at depth={}, code={}, new bit_len={}", depth, code, bit_len);
            let count = Self::build_condensed_pattern_table(
                &mut new_table,
                depths,
                patterns,
                0,
                0,
                depth,  // Keep the same depth, don't restart
                max_depth,
            )?;
            
            let cw = CodeWord {
                pattern: vec![],
                code,
                len: 0,
                ptr: Some(new_table),
            };
            println!("    Inserting pointer to deeper table at code={}", code);
            table.insert_word(cw);
            return Ok(count);
        }
        
        // Check if we've exceeded max depth
        if max_depth == 0 {
            return Ok(0);
        }

        // Now we can safely subtract since we checked max_depth > 0 above
        let remaining_depth = max_depth - 1;
        
        let b0 = Self::build_condensed_pattern_table(
            table,
            depths,
            patterns,
            code,
            bits + 1,
            depth + 1,
            remaining_depth,
        )?;
        
        let b1 = Self::build_condensed_pattern_table(
            table,
            &depths[b0..],
            &patterns[b0..],
            (1 << bits) | code,
            bits + 1,
            depth + 1,
            remaining_depth,
        )?;
        
        Ok(b0 + b1)
    }

    fn build_pos_dict(data: &[u8]) -> Result<PosTable> {
        // println!("Building position dict from {} bytes", data.len());
        let mut depths = Vec::new();
        let mut positions = Vec::new();
        let mut pos = 0;
        let mut max_depth = 0u64;

        while pos < data.len() {
            let (depth, n) = decode_varint(&data[pos..])?;
            if depth > MAX_ALLOWED_DEPTH {
                return Err(Error::InvalidFormat(format!("Pos depth {} exceeds max", depth)));
            }
            if depth > max_depth {
                max_depth = depth;
            }
            depths.push(depth);
            pos += n;

            let (position, n) = decode_varint(&data[pos..])?;
            pos += n;
            positions.push(position);
            println!("  Parsed position entry: depth={}, position={}", depth, position);
        }
        
        println!("  Parsed {} positions, max_depth={}", positions.len(), max_depth);
        if positions.len() > 0 {
            println!("  First few positions: {:?}", &positions[..positions.len().min(5)]);
        }

        let bit_len = if max_depth > 9 { 9 } else { max_depth as usize };
        let table_size = 1 << bit_len;
        println!("Creating pos table: bit_len={}, table_size={}, max_depth={}", bit_len, table_size, max_depth);
        let mut table = PosTable {
            bit_len,
            pos: vec![0; table_size],
            lens: vec![0; table_size],
            ptrs: (0..table_size).map(|_| None).collect(),
        };

        Self::build_pos_table(&mut table, &depths, &positions, 0, 0, 0, max_depth)?;
        Ok(table)
    }

    fn build_pos_table(
        table: &mut PosTable,
        depths: &[u64],
        positions: &[u64],
        code: u16,
        bits: usize,
        depth: u64,
        max_depth: u64,
    ) -> Result<usize> {
        if depths.is_empty() {
            return Ok(0);
        }

        // Stop recursion if we've gone past the next item
        if depth > depths[0] {
            return Ok(0);
        }

        if depth == depths[0] {
            let p = positions[0];
            println!("    Building pos table entry: code={} (0b{:b}), bits={}, pos={}, table.bit_len={}", 
                     code, code, bits, p, table.bit_len);
            if table.bit_len == bits {
                table.pos[code as usize] = p;
                table.lens[code as usize] = bits as u8;
                println!("      Set table[{}] = pos={}, bits={}", code, p, bits);
            } else {
                let code_step = 1u16 << bits;
                let code_to = code | (1u16 << table.bit_len);
                let mut c = code;
                while c < code_to {
                    table.pos[c as usize] = p;
                    table.lens[c as usize] = bits as u8;
                    println!("      Set table[{}] = pos={}, bits={}", c, p, bits);
                    c += code_step;
                }
            }
            return Ok(1);
        }

        if bits == 9 {
            let bit_len = if max_depth > 9 { 9 } else { max_depth as usize };
            let table_size = 1 << bit_len;
            let mut new_table = Box::new(PosTable {
                bit_len,
                pos: vec![0; table_size],
                lens: vec![0; table_size],
                ptrs: (0..table_size).map(|_| None).collect(),
            });
            
            let count = Self::build_pos_table(
                &mut new_table,
                depths,
                positions,
                0,
                0,
                depth,
                max_depth,
            )?;
            
            table.ptrs[code as usize] = Some(new_table);
            return Ok(count);
        }

        // Now we can safely subtract since we checked max_depth > 0 above
        let remaining_depth = max_depth - 1;
        
        println!("    Recursing: depth={}, bits={}, code={:b}, depths[0]={}", depth, bits, code, depths[0]);
        
        let b0 = Self::build_pos_table(
            table,
            depths,
            positions,
            code,
            bits + 1,
            depth + 1,
            remaining_depth,
        )?;
        
        let b1 = Self::build_pos_table(
            table,
            &depths[b0..],
            &positions[b0..],
            (1 << bits) | code,
            bits + 1,
            depth + 1,
            remaining_depth,
        )?;
        
        Ok(b0 + b1)
    }

    pub fn make_getter(&'a self) -> Getter<'a> {
        Getter {
            pattern_dict: self.dict.as_ref(),
            pos_dict: self.pos_dict.as_ref(),
            f_name: String::new(),
            data: self.data,
            data_p: self.words_start,
            data_bit: 0,
            trace: false,
        }
    }
    
    pub fn words_start(&self) -> usize {
        self.words_start as usize
    }

    pub fn words_count(&self) -> u64 {
        self.words_count
    }
}

impl PatternTable {
    fn new(bit_len: usize) -> Self {
        let size = 1 << bit_len;
        PatternTable {
            patterns: (0..size).map(|_| None).collect(),
            bit_len,
        }
    }

    fn insert_word(&mut self, cw: CodeWord) {
        if self.bit_len <= CONDENSE_PATTERN_TABLE_BIT_THRESHOLD {
            let code_step = 1u16 << cw.len;
            let code_from = cw.code;
            let code_to = if self.bit_len != cw.len as usize && cw.len > 0 {
                cw.code | (1u16 << self.bit_len)
            } else {
                cw.code + code_step
            };

            // Debug output disabled for now
            // println!("      insert_word: code_from={}, code_to={}, code_step={}, pattern={:?}", 
            //          code_from, code_to, code_step, String::from_utf8_lossy(&cw.pattern));

            // For entries with pointers to deeper tables, we need special handling
            if cw.len == 0 && cw.ptr.is_some() {
                // This is a pointer to a deeper table - just insert it once at the exact code
                self.patterns[code_from as usize] = Some(cw);
            } else {
                // Regular pattern - replicate across the range
                let mut c = code_from;
                while c < code_to {
                    self.patterns[c as usize] = Some(CodeWord {
                        pattern: cw.pattern.clone(),
                        code: cw.code,
                        len: cw.len,
                        ptr: None, // Regular patterns don't have pointers
                    });
                    c += code_step;
                }
            }
        }
    }

    fn condensed_table_search(&self, code: u16) -> Option<&CodeWord> {
        if self.bit_len <= CONDENSE_PATTERN_TABLE_BIT_THRESHOLD {
            let result = self.patterns[code as usize].as_ref();
            if let Some(cw) = result {
                // println!("        condensed_table_search: code={} -> found pattern len={}, data={:?}", 
                //          code, cw.len, String::from_utf8_lossy(&cw.pattern));
            } else {
                // println!("        condensed_table_search: code={} -> None", code);
            }
            return result;
        }
        // println!("        condensed_table_search: bit_len={} > threshold, returning None", self.bit_len);
        None
    }
}

impl<'a> Getter<'a> {
    pub fn reset(&mut self, offset: u64) {
        self.data_p = offset;
        self.data_bit = 0;
    }

    pub fn has_next(&self) -> bool {
        self.data_p < self.data.len() as u64
    }

    pub fn match_prefix(&mut self, prefix: &[u8]) -> bool {
        // Port of Go's MatchPrefix - saves position and restores it after checking
        let save_data_p = self.data_p;
        let save_data_bit = self.data_bit;
        
        // Always restore position after prefix check (Go behavior)  
        let result = match self.next(&mut Vec::new()) {
            Ok(word) => {
                if prefix.is_empty() {
                    true  // Empty prefix always matches
                } else {
                    word.len() >= prefix.len() && &word[..prefix.len()] == prefix
                }
            }
            Err(_) => false,
        };
        
        // Always restore position (Go MatchPrefix never advances)
        self.data_p = save_data_p;
        self.data_bit = save_data_bit;
        
        result
    }

    pub fn match_cmp(&mut self, target: &[u8]) -> i32 {
        // Port of Go's MatchCmp - compares and only advances on exact match
        let save_data_p = self.data_p;
        let save_data_bit = self.data_bit;
        
        match self.next(&mut Vec::new()) {
            Ok(word) => {
                let result = if word.as_slice() < target {
                    -1
                } else if word.as_slice() > target {
                    1
                } else {
                    0  // Exact match
                };
                
                // Go behavior: restore position unless exact match
                if result != 0 {
                    self.data_p = save_data_p;
                    self.data_bit = save_data_bit;
                }
                // If result == 0 (exact match), keep advanced position
                
                result
            }
            Err(_) => {
                // Restore position on error
                self.data_p = save_data_p;
                self.data_bit = save_data_bit;
                -1
            }
        }
    }

    pub fn next_with_pos(&mut self, buf: &mut Vec<u8>) -> Result<(Vec<u8>, u64)> {
        let start_pos = self.data_p;
        let word = self.next(buf)?;
        let end_pos = self.data_p;
        Ok((word, end_pos as u64))
    }

    pub fn next(&mut self, _buf: &mut Vec<u8>) -> Result<Vec<u8>> {
        // Direct port of Go's Next() - decompress.go lines 669-733
        let save_pos = self.data_p;
        let mut word_len = self.next_pos(true)?;
        word_len = if word_len > 0 { word_len - 1 } else { 0 }; // because when create huffman tree we do ++, because 0 is terminator
        
        if word_len == 0 {
            // Handle empty word case - still need to read terminator but no data
            // For the case with no patterns, use the simple NextUncompressed approach
            if self.pattern_dict.is_none() {
                // Read terminator position (should be 0)
                let terminator = self.next_pos(false)?;
                println!("    Empty word - read terminator: {}", terminator);
                
                // Switch to data mode if needed but don't read any data
                // Empty word - no data needed
                
                return Ok(vec![]);
            } else {
                // Handle empty word with patterns - same logic but no data to read
                loop {
                    let pos = self.next_pos(false)?;
                    if pos == 0 {
                        break;
                    }
                    // Skip any patterns
                    if let Some(dict) = self.pattern_dict {
                        self.next_pattern(dict)?;
                    }
                }
                
                // Empty word with patterns - no data needed
                
                return Ok(vec![]);
            }
        }
        
        // For the case with no patterns, use the simple NextUncompressed approach
        if self.pattern_dict.is_none() {
            // Read terminator position (should be 0)
            let terminator = self.next_pos(false)?;
            println!("    Read terminator: {}", terminator);
            
            // Switch to data mode if this is the first time reading data
            // After reading positions, align to byte boundary for data
            if self.data_bit > 0 {
                self.data_p += 1;
                self.data_bit = 0;
                println!("    Aligned to byte boundary for data: data_p={}", self.data_p);
            }
            
            println!("    Reading {} bytes from data_p={}", word_len, self.data_p);
            
            // Read word data directly
            if self.data_p + word_len as u64 > self.data.len() as u64 {
                return Err(Error::InvalidFormat("Word extends beyond data".into()));
            }
            
            let word = self.data[self.data_p as usize..(self.data_p + word_len as u64) as usize].to_vec();
            self.data_p += word_len as u64;
            
            println!("    NO PATTERN DICT: Returning word: {:?} ('{}') len={}", word, String::from_utf8_lossy(&word), word.len());
            return Ok(word);
        }
        
        // Handle pattern dictionary case - implement the full two-pass algorithm from Go
        let save_data_p_2 = self.data_p;
        let save_data_bit_2 = self.data_bit;
        
        let mut result = vec![0u8; word_len];
        
        // First pass: place patterns in the word buffer
        let mut buf_pos = 0usize;
        let mut pattern_count = 0;
        println!("    First pass: placing patterns");
        loop {
            let pos = self.next_pos(false)?;
            println!("    First pass: read pos={}", pos);
            if pos == 0 {
                println!("    First pass: terminator found, ending loop");
                break;
            }
            
            pattern_count += 1;
            if pattern_count > 100 { // Safety check
                return Err(Error::InvalidFormat("Too many patterns, possible infinite loop".into()));
            }
            
            buf_pos += pos - 1; // Positions are relative to each other
            println!("    First pass: buf_pos now = {}", buf_pos);
            
            if let Some(dict) = self.pattern_dict {
                println!("    About to read pattern with dict at data_p={}, data_bit={}", self.data_p, self.data_bit);
                let pattern = self.next_pattern(dict)?;
                println!("    First pass: decoded pattern {:?} at buf_pos={}", String::from_utf8_lossy(&pattern), buf_pos);
                if buf_pos < result.len() && buf_pos + pattern.len() <= result.len() {
                    result[buf_pos..buf_pos + pattern.len()].copy_from_slice(&pattern);
                    println!("      Placed pattern at buf_pos={}", buf_pos);
                } else {
                    println!("      Pattern out of bounds: buf_pos={}, pattern_len={}, result_len={}", buf_pos, pattern.len(), result.len());
                }
            }
        }
        
        // CRITICAL: Do NOT switch to data mode yet. The position stream continues to be used
        // for the second pass. Only after both passes do we switch to reading uncovered data.
        
        // Reset position stream for second pass 
        self.data_p = save_data_p_2;
        self.data_bit = save_data_bit_2;
        self.next_pos(true)?; // Reset huffman reader state for second pass
        
        // Second pass: identify uncovered areas and read uncovered data from the data stream
        buf_pos = 0usize;
        let mut last_uncovered = 0usize;
        let mut uncovered_data_offset = 0usize;
        
        // Determine where uncovered data starts in the file
        let uncovered_data_start = if self.data_bit > 0 {
            self.data_p + 1
        } else {
            self.data_p
        };
        
        println!("    Second pass: filling uncovered bytes, data starts at {}", uncovered_data_start);
        
        loop {
            let pos = self.next_pos(false)?;
            if pos == 0 {
                break;
            }
            
            buf_pos += pos - 1;
            
            // Fill gap before pattern with uncovered data
            if buf_pos > last_uncovered {
                let gap_size = buf_pos - last_uncovered;
                println!("      Filling gap at [{}..{}] with {} bytes from offset {}", 
                         last_uncovered, buf_pos, gap_size, uncovered_data_offset);
                
                if uncovered_data_start + uncovered_data_offset as u64 + gap_size as u64 <= self.data.len() as u64 {
                    result[last_uncovered..buf_pos].copy_from_slice(
                        &self.data[(uncovered_data_start + uncovered_data_offset as u64) as usize..
                                  (uncovered_data_start + uncovered_data_offset as u64 + gap_size as u64) as usize]
                    );
                    uncovered_data_offset += gap_size;
                }
            }
            
            // Skip over the pattern (consume from both position and pattern streams)
            if let Some(dict) = self.pattern_dict {
                let pattern = self.next_pattern(dict)?;
                last_uncovered = buf_pos + pattern.len();
                println!("      Skipped pattern of length {}, last_uncovered now = {}", pattern.len(), last_uncovered);
            }
        }
        
        // Fill remaining uncovered bytes at the end
        if word_len > last_uncovered {
            let remaining_size = word_len - last_uncovered;
            println!("      Filling remaining gap at [{}..{}] with {} bytes", 
                     last_uncovered, word_len, remaining_size);
            
            if uncovered_data_start + uncovered_data_offset as u64 + remaining_size as u64 <= self.data.len() as u64 {
                result[last_uncovered..word_len].copy_from_slice(
                    &self.data[(uncovered_data_start + uncovered_data_offset as u64) as usize..
                              (uncovered_data_start + uncovered_data_offset as u64 + remaining_size as u64) as usize]
                );
                uncovered_data_offset += remaining_size;
            }
        }
        
        // NOW we can update the data pointer for the next word
        self.data_p = uncovered_data_start + uncovered_data_offset as u64;
        self.data_bit = 0;
        // Data mode tracking no longer needed with single stream
        
        println!("    Pattern case - returning word: {:?} ('{}')", result, String::from_utf8_lossy(&result));
        Ok(result)
    }
    
    pub fn skip(&mut self) -> Result<()> {
        if !self.has_next() {
            return Ok(());
        }
        
        let mut word_len = self.next_pos(true)?;
        
        if word_len == 0 {
            return Ok(());
        }
        
        word_len -= 1; // Adjust for encoding

        if word_len == 0 {
            if self.data_bit > 0 {
                self.data_p += 1;
                self.data_bit = 0;
            }
            return Ok(());
        }

        // When there's no dictionary, just skip the bytes
        if self.pattern_dict.is_none() && self.pos_dict.is_none() {
            self.data_p += word_len as u64;
            return Ok(());
        }
        
        // Skip pattern positions
        while self.next_pos(false)? != 0 {
            // Skip patterns
            if let Some(dict) = self.pattern_dict {
                self.next_pattern(dict)?;
            }
        }
        
        // TODO: Skip remaining uncovered bytes properly
        // For now, this is a simplified implementation
        
        Ok(())
    }

    fn next_pos(&mut self, clean: bool) -> Result<usize> {
        if let Some(pos_dict) = self.pos_dict {
            let result = self.next_pos_internal(pos_dict, clean)?;
            println!("    next_pos with dict returned: {}", result);
            Ok(result)
        } else {
            // When no position dictionary, read varint
            if self.data_p >= self.data.len() as u64 {
                return Ok(0);
            }
            let (val, n) = decode_varint(&self.data[self.data_p as usize..])?;
            self.data_p += n as u64;
            // println!("    next_pos varint returned: {}", val);
            Ok(val as usize)
        }
    }

    fn next_pos_internal(&mut self, table: &PosTable, _clean: bool) -> Result<usize> {
        // Use single stream pointer for reading positions
        
        if table.bit_len == 0 {
            return Ok(table.pos[0] as usize);
        }
        
        loop {
            if self.data_p >= self.data.len() as u64 {
                return Ok(0);
            }
            
            // Read bits from the stream
            let mut code = (self.data[self.data_p as usize] >> self.data_bit) as u16;
            if 8 - self.data_bit < table.bit_len && (self.data_p + 1) < self.data.len() as u64 {
                code |= (self.data[(self.data_p + 1) as usize] as u16) << (8 - self.data_bit);
            }
            code &= (1u16 << table.bit_len) - 1;
            
            let l = table.lens[code as usize];
            println!("    next_pos_internal: data_p={}, data_bit={}, code={}, bits={}, pos={}", 
                     self.data_p, self.data_bit, code, l, 
                     if l > 0 { table.pos[code as usize] } else { 0 });
            
            if l == 0 {
                // Follow pointer to deeper table
                if let Some(ptr) = &table.ptrs[code as usize] {
                    self.data_bit += 9;
                    self.data_p += (self.data_bit / 8) as u64;
                    self.data_bit %= 8;
                    return self.next_pos_internal(ptr, false);
                } else {
                    return Ok(0);
                }
            } else {
                // Found position
                self.data_bit += l as usize;
                let pos = table.pos[code as usize];
                self.data_p += (self.data_bit / 8) as u64;
                self.data_bit %= 8;
                return Ok(pos as usize);
            }
        }
    }

    fn next_pattern(&mut self, table: &PatternTable) -> Result<Vec<u8>> {
        println!("      next_pattern: data_p={}, data_bit={}, table.bit_len={}", self.data_p, self.data_bit, table.bit_len);
        
        if table.bit_len == 0 {
            return Ok(table.patterns[0].as_ref().map(|cw| cw.pattern.clone()).unwrap_or_default());
        }
        
        loop {
            if self.data_p >= self.data.len() as u64 {
                return Ok(vec![]);
            }
            
            // Read bits like Go version
            let mut code = (self.data[self.data_p as usize] >> self.data_bit) as u16;
            if 8 - self.data_bit < table.bit_len && (self.data_p + 1) < self.data.len() as u64 {
                code |= (self.data[(self.data_p + 1) as usize] as u16) << (8 - self.data_bit);
            }
            code &= (1u16 << table.bit_len) - 1;
            
            println!("      Read pattern code={} (0x{:x}) from data_p={}, data_bit={}, table.bit_len={}", 
                     code, code, self.data_p, self.data_bit, table.bit_len);
            
            if let Some(cw) = table.condensed_table_search(code) {
                let l = cw.len;
                println!("      Found codeword: len={}, pattern={:?}", l, String::from_utf8_lossy(&cw.pattern));
                if l == 0 {
                    // Follow pointer to deeper table
                    if let Some(ptr) = &cw.ptr {
                        println!("      Following pointer to deeper table");
                        self.data_bit += 9; // Always advance by 9 bits for table pointers
                        self.data_p += (self.data_bit / 8) as u64;
                        self.data_bit %= 8;
                        return self.next_pattern(ptr);
                    } else {
                        return Ok(vec![]);
                    }
                } else {
                    // Found pattern
                    self.data_bit += l as usize;
                    self.data_p += (self.data_bit / 8) as u64;
                    self.data_bit %= 8;
                    println!("      Returning pattern: {:?}", String::from_utf8_lossy(&cw.pattern));
                    return Ok(cw.pattern.clone());
                }
            } else {
                return Err(Error::InvalidFormat(format!("Pattern not found for code {}", code)));
            }
        }
    }

    fn peek_bits(&self, n: usize) -> Result<u16> {
        if n > 16 {
            return Err(Error::InvalidFormat("Cannot peek more than 16 bits".into()));
        }

        let mut result = 0u16;
        let mut p = self.data_p;
        let mut bit = self.data_bit;

        for i in 0..n {
            if p >= self.data.len() as u64 {
                break;
            }

            // Read bits from LSB to MSB to match writer's bit order
            if (self.data[p as usize] >> bit) & 1 != 0 {
                result |= 1 << i;
            }

            bit += 1;
            if bit == 8 {
                bit = 0;
                p += 1;
            }
        }

        Ok(result)
    }

    fn skip_bits(&mut self, n: usize) {
        self.data_bit += n;
        self.data_p += (self.data_bit / 8) as u64;
        self.data_bit %= 8;
    }
}

fn decode_varint(data: &[u8]) -> Result<(u64, usize)> {
    let mut result = 0u64;
    let mut shift = 0;
    
    for (i, &byte) in data.iter().enumerate() {
        if i == 9 && byte > 1 {
            return Err(Error::InvalidFormat("Varint too large".into()));
        }
        
        result |= ((byte & 0x7F) as u64) << shift;
        
        if byte & 0x80 == 0 {
            return Ok((result, i + 1));
        }
        
        shift += 7;
    }
    
    Err(Error::InvalidFormat("Varint missing terminator".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_decode() {
        let data = vec![0x01];
        let (val, n) = decode_varint(&data).unwrap();
        assert_eq!(val, 1);
        assert_eq!(n, 1);

        let data = vec![0x80, 0x01];
        let (val, n) = decode_varint(&data).unwrap();
        assert_eq!(val, 128);
        assert_eq!(n, 2);
    }
}