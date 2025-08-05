use crate::error::{Error, Result};
use std::path::Path;
use memmap2::{Mmap, MmapOptions};
use std::fs::File;

/// Erigon segment decompressor implementation
/// Based on the dictionary compression algorithm used in erigon-lib/seg
/// 
/// The compression format uses:
/// - Huffman-tree based pattern matching for common byte sequences
/// - Position encoding for pattern locations
/// - Variable-length encoding for positions and patterns

const MAX_ALLOWED_DEPTH: u64 = 50;
const COMPRESSED_MIN_SIZE: usize = 32;
const CONDENSE_PATTERN_TABLE_BIT_THRESHOLD: usize = 9;

#[derive(Debug)]
pub struct Decompressor<'a> {
    data: &'a [u8],
    words_count: u64,
_empty_words_count: u64,
    dict: Option<PatternTable>,
    pos_dict: Option<PosTable>,
    words_start: usize,
}

/// A decompressor that owns its data
#[derive(Debug)]
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

/// Pattern table for Huffman decoding
#[derive(Debug)]
struct PatternTable {
    patterns: Vec<Option<CodeWord>>,
    bit_len: usize,
}

/// A codeword in the Huffman tree
#[derive(Debug)]
struct CodeWord {
    pattern: Vec<u8>,
    code: u16,
    len: u8,
    ptr: Option<Box<PatternTable>>,
}

/// Position table for decoding positions
#[derive(Debug)]
struct PosTable {
    pos: Vec<u64>,
    lens: Vec<u8>,
    ptrs: Vec<Option<Box<PosTable>>>,
    bit_len: usize,
}

pub struct Getter<'a> {
    data: &'a [u8],
    pattern_dict: Option<&'a PatternTable>,
    pos_dict: Option<&'a PosTable>,
    data_p: usize,
    data_bit: usize,
    words_start: usize,
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
            words_count,
_empty_words_count: empty_words_count,
            dict,
            pos_dict,
            words_start,
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
        
        // println!("Parsed {} patterns, max_depth={}", patterns.len(), max_depth);

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

        // Stop recursion if we've exceeded max depth
        if max_depth == 0 || depth > depths[0] {
            return Ok(0);
        }

        if depth == depths[0] {
            let cw = CodeWord {
                pattern: patterns[0].clone(),
                code,
                len: bits as u8,
                ptr: None,
            };
            table.insert_word(cw);
            return Ok(1);
        }

        if bits == 9 {
            let bit_len = if max_depth > 9 { 9 } else { max_depth as usize };
            let mut new_table = Box::new(PatternTable::new(bit_len));
            let count = Self::build_condensed_pattern_table(
                &mut new_table,
                depths,
                patterns,
                0,
                0,
                depth,
                max_depth,
            )?;
            
            let cw = CodeWord {
                pattern: vec![],
                code,
                len: 0,
                ptr: Some(new_table),
            };
            table.insert_word(cw);
            return Ok(count);
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

        // Stop recursion if we've exceeded max depth
        if max_depth == 0 || depth > depths[0] {
            return Ok(0);
        }

        if depth == depths[0] {
            let p = positions[0];
            if table.bit_len == bits {
                table.pos[code as usize] = p;
                table.lens[code as usize] = bits as u8;
            } else {
                let code_step = 1u16 << bits;
                let code_to = code | (1u16 << table.bit_len);
                let mut c = code;
                while c < code_to {
                    table.pos[c as usize] = p;
                    table.lens[c as usize] = bits as u8;
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
            data: self.data, // Keep full data, use words_start in reset
            pattern_dict: self.dict.as_ref(),
            pos_dict: self.pos_dict.as_ref(),
            data_p: self.words_start,
            data_bit: 0,
            words_start: self.words_start,
        }
    }
    
    pub fn words_start(&self) -> usize {
        self.words_start
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

            let mut c = code_from;
            while c < code_to {
                self.patterns[c as usize] = Some(CodeWord {
                    pattern: cw.pattern.clone(),
                    code: cw.code,
                    len: cw.len,
                    ptr: None, // Cloning deep pattern tables is complex; for now skip
                });
                c += code_step;
            }
        }
    }

    fn condensed_table_search(&self, code: u16) -> Option<&CodeWord> {
        if self.bit_len <= CONDENSE_PATTERN_TABLE_BIT_THRESHOLD {
            return self.patterns[code as usize].as_ref();
        }
        None
    }
}

impl<'a> Getter<'a> {
    pub fn reset(&mut self, offset: u64) {
        // The offset is relative to the words section
        self.data_p = self.words_start + offset as usize;
        self.data_bit = 0;
    }

    pub fn has_next(&self) -> bool {
        self.data_p < self.data.len()
    }

    pub fn next(&mut self, _buf: &mut Vec<u8>) -> Result<Vec<u8>> {
        if !self.has_next() {
            return Ok(vec![]);
        }

        let _save_pos = self.data_p;
        let mut word_len = self.next_pos(true)?;
        
        println!("  next_pos returned word_len: {}, data_p: {}, save_pos: {}", word_len, self.data_p, _save_pos);
        
        if word_len == 0 {
            // Check if we should return empty vector or treat as error
            if self.data_bit > 0 {
                self.data_p += 1;
                self.data_bit = 0;
            }
            return Ok(vec![]);
        }
        
        word_len -= 1; // Adjust for encoding

        if word_len == 0 {
            if self.data_bit > 0 {
                self.data_p += 1;
                self.data_bit = 0;
            }
            return Ok(vec![]);
        }

        // Special case: no dictionaries at all, just read raw bytes
        if self.pattern_dict.is_none() && self.pos_dict.is_none() {
            println!("  Reading {} raw bytes from position {}", word_len, self.data_p);
            let mut result = Vec::with_capacity(word_len);
            for _ in 0..word_len {
                if self.data_p >= self.data.len() {
                    break;
                }
                result.push(self.data[self.data_p]);
                self.data_p += 1;
            }
            return Ok(result);
        }
        
        // Erigon format: even without pattern dictionary, positions are used
        // to mark where patterns would go. The uncovered bytes are stored after.
        
        let mut result = vec![0u8; word_len];
        let mut buf_pos = 0;
        let mut last_uncovered = 0;
        let _post_loop_start = self.data_p;
        
        // First pass: read all positions (even if no patterns)
        loop {
            let pos = self.next_pos(false)?;
            if pos == 0 {
                break; // End of positions
            }
            
            buf_pos += pos - 1; // Positions are relative to each other
            
            if let Some(dict) = self.pattern_dict {
                let (pattern, _) = self.next_pattern(dict)?;
                if buf_pos + pattern.len() <= word_len {
                    result[buf_pos..buf_pos + pattern.len()].copy_from_slice(&pattern);
                    last_uncovered = buf_pos + pattern.len();
                }
            } else {
                // No pattern dictionary, but we still need to track position
                last_uncovered = buf_pos;
            }
        }
        
        // Align to byte boundary after reading positions
        if self.data_bit > 0 {
            self.data_p += 1;
            self.data_bit = 0;
        }
        
        let post_loop_pos = self.data_p;
        
        // Second pass: fill uncovered bytes from the data stream
        println!("  After positions: last_uncovered={}, word_len={}, post_loop_pos={}", last_uncovered, word_len, post_loop_pos);
        if last_uncovered < word_len {
            let remaining = word_len - last_uncovered;
            if post_loop_pos + remaining <= self.data.len() {
                println!("  Reading {} uncovered bytes from position {}", remaining, post_loop_pos);
                result[last_uncovered..].copy_from_slice(&self.data[post_loop_pos..post_loop_pos + remaining]);
                self.data_p = post_loop_pos + remaining;
            } else {
                println!("  Not enough data: need {} bytes but only {} available", remaining, self.data.len() - post_loop_pos);
            }
        }
        
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
            self.data_p += word_len;
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
            if self.data_p >= self.data.len() {
                return Ok(0);
            }
            let (val, n) = decode_varint(&self.data[self.data_p..])?;
            self.data_p += n;
            // println!("    next_pos varint returned: {}", val);
            Ok(val as usize)
        }
    }

    fn next_pos_internal(&mut self, table: &PosTable, clean: bool) -> Result<usize> {
        if self.data_p >= self.data.len() {
            return Ok(0);
        }

        let code = self.peek_bits(table.bit_len)?;
        let bits = table.lens[code as usize];
        
        println!("    next_pos_internal: code={}, bits={}, pos={}", code, bits, 
                 if bits > 0 { table.pos[code as usize] } else { 0 });
        
        if let Some(ptr) = &table.ptrs[code as usize] {
            self.skip_bits(9);
            return self.next_pos_internal(ptr, clean);
        }

        if bits == 0 {
            return Ok(0);
        }

        self.skip_bits(bits as usize);
        Ok(table.pos[code as usize] as usize)
    }

    fn next_pattern(&mut self, table: &PatternTable) -> Result<(Vec<u8>, usize)> {
        let code = self.peek_bits(table.bit_len)?;
        
        if let Some(cw) = table.condensed_table_search(code) {
            if let Some(ptr) = &cw.ptr {
                self.skip_bits(9);
                return self.next_pattern(ptr);
            }
            
            self.skip_bits(cw.len as usize);
            Ok((cw.pattern.clone(), cw.pattern.len()))
        } else {
            Err(Error::InvalidFormat("Pattern not found".into()))
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
            if p >= self.data.len() {
                break;
            }

            // Read bits from LSB to MSB to match writer's bit order
            if (self.data[p] >> bit) & 1 != 0 {
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
        self.data_p += self.data_bit / 8;
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