// Port of Erigon's decompress.go
// Original: go/src/decompress.go

use crate::error::CompressionError;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::time::SystemTime;

// From Go: decompress.go:39
type Word = Vec<u8>; // plain text word associated with code from dictionary

// From Go: decompress.go:41
#[derive(Debug)]
struct Codeword {
    pattern: Word,                  // Pattern corresponding to entries
    ptr: Option<Box<PatternTable>>, // pointer to deeper level tables
    code: u16,                      // code associated with that word
    len: u8,                        // Number of bits in the codes
}

// From Go: decompress.go:48
#[derive(Debug)]
struct PatternTable {
    patterns: Vec<Option<Codeword>>,
    bit_len: usize, // Number of bits to lookup in the table
}

impl PatternTable {
    // From Go: decompress.go:53
    fn new(bit_len: usize) -> Self {
        let size = if bit_len <= CONDENSE_PATTERN_TABLE_BIT_THRESHOLD {
            1 << bit_len
        } else {
            0 // Will use vec for sparse storage
        };

        PatternTable {
            patterns: (0..size).map(|_| None).collect(),
            bit_len,
        }
    }

    // From Go: decompress.go:63
    fn insert_word(&mut self, cw: Codeword) {
        if self.bit_len <= CONDENSE_PATTERN_TABLE_BIT_THRESHOLD {
            let code_step = (1u16) << cw.len;
            let code_from = cw.code;
            let code_to = if self.bit_len != cw.len as usize && cw.len > 0 {
                code_from | (1u16 << self.bit_len)
            } else {
                cw.code + code_step
            };

            let mut c = code_from;
            while c < code_to {
                // Store reference to the same codeword
                let stored_cw = Codeword {
                    pattern: cw.pattern.clone(),
                    ptr: None, // ptr is always None for simple codewords
                    code: cw.code,
                    len: cw.len,
                };
                self.patterns[c as usize] = Some(stored_cw);
                c += code_step;
            }
        } else {
            // For sparse tables, just append
            self.patterns.push(Some(cw));
        }
    }

    // From Go: decompress.go:80
    fn condensed_table_search(&self, code: u16) -> Option<&Codeword> {
        if self.bit_len <= CONDENSE_PATTERN_TABLE_BIT_THRESHOLD {
            self.patterns.get(code as usize)?.as_ref()
        } else {
            // Linear search for sparse tables
            for pattern in &self.patterns {
                if let Some(cw) = pattern {
                    if cw.code == code {
                        return Some(cw);
                    }
                    let d = code - cw.code;
                    if d & 1 != 0 {
                        continue;
                    }
                    if check_distance(cw.len as usize, d as usize) {
                        return Some(cw);
                    }
                }
            }
            None
        }
    }
}

// From Go: decompress.go:99
#[derive(Debug)]
struct PosTable {
    pos: Vec<u64>,
    lens: Vec<u8>,
    ptrs: Vec<Option<Box<PosTable>>>,
    bit_len: usize,
}

impl PosTable {
    fn new(bit_len: usize) -> Self {
        let size = 1 << bit_len;
        PosTable {
            pos: vec![0; size],
            lens: vec![0; size],
            ptrs: (0..size).map(|_| None).collect(),
            bit_len,
        }
    }
}

// From Go: decompress.go:121
pub struct Decompressor {
    f: Option<File>,
    dict: Option<PatternTable>,
    pos_dict: Option<PosTable>,
    data: Vec<u8>,
    words_start: u64,
    size: i64,
    mod_time: SystemTime,
    words_count: u64,
    empty_words_count: u64,
    serialized_dict_size: u64,
    dict_words: usize,
    file_path: String,
    file_name: String,
}

// From Go: decompress.go:140-146
const MAX_ALLOWED_DEPTH: u64 = 50;
const COMPRESSED_MIN_SIZE: usize = 32;

// From Go: decompress.go:156
const CONDENSE_PATTERN_TABLE_BIT_THRESHOLD: usize = 9;

impl Decompressor {
    // From Go: decompress.go:177
    pub fn new(compressed_file_path: impl AsRef<Path>) -> Result<Self, CompressionError> {
        let path = compressed_file_path.as_ref();
        let file_name = path
            .file_name()
            .ok_or_else(|| CompressionError::Other("Invalid file path".to_string()))?
            .to_string_lossy()
            .to_string();

        let mut f = File::open(path)?;
        let metadata = f.metadata()?;
        let size = metadata.len() as i64;

        if size < COMPRESSED_MIN_SIZE as i64 {
            return Err(CompressionError::Other(format!(
                "File {} too small: {} bytes, expected at least {} bytes",
                file_name, size, COMPRESSED_MIN_SIZE
            )));
        }

        let mut data = Vec::with_capacity(size as usize);
        f.read_to_end(&mut data)?;

        // Read header
        let words_count = u64::from_be_bytes(data[0..8].try_into().unwrap());
        let empty_words_count = u64::from_be_bytes(data[8..16].try_into().unwrap());
        let pattern_dict_size = u64::from_be_bytes(data[16..24].try_into().unwrap());

        if 24 + pattern_dict_size > size as u64 {
            return Err(CompressionError::Other(format!(
                "Invalid pattern dictionary size {} in file {}",
                pattern_dict_size, file_name
            )));
        }

        // Parse pattern dictionary
        let pattern_dict_data = &data[24..24 + pattern_dict_size as usize];
        let (dict, dict_words) = parse_pattern_dictionary(pattern_dict_data)?;

        // Read position dictionary size
        let pos_dict_start = 24 + pattern_dict_size as usize;
        if pos_dict_start + 8 > size as usize {
            return Err(CompressionError::Other(format!(
                "File {} too small to contain position dictionary size",
                file_name
            )));
        }
        let pos_dict_size =
            u64::from_be_bytes(data[pos_dict_start..pos_dict_start + 8].try_into().unwrap());

        if pos_dict_start + 8 + pos_dict_size as usize > size as usize {
            return Err(CompressionError::Other(format!(
                "Invalid position dictionary size {} in file {}",
                pos_dict_size, file_name
            )));
        }

        // Parse position dictionary
        let pos_dict_data = &data[pos_dict_start + 8..pos_dict_start + 8 + pos_dict_size as usize];
        let pos_dict = parse_position_dictionary(pos_dict_data)?;
        log::debug!(
            "Position dict: bit_len: {}, pos[0]: {:?}",
            pos_dict.bit_len,
            if pos_dict.pos.is_empty() {
                None
            } else {
                Some(pos_dict.pos[0])
            }
        );

        let words_start_offset = 8 + pos_dict_size; // 8 for pos dict size + pos dict data
        let words_start = 24 + pattern_dict_size + words_start_offset;

        log::debug!(
            "Decompressor initialized: words_start: {}, file_size: {}",
            words_start,
            size
        );

        Ok(Decompressor {
            f: Some(f),
            dict: Some(dict),
            pos_dict: Some(pos_dict),
            data,
            words_start,
            size,
            mod_time: metadata.modified()?,
            words_count,
            empty_words_count,
            serialized_dict_size: pattern_dict_size,
            dict_words,
            file_path: path.to_string_lossy().to_string(),
            file_name,
        })
    }

    // From Go: decompress.go:642-643
    pub fn count(&self) -> usize {
        self.words_count as usize
    }

    pub fn empty_words_count(&self) -> usize {
        self.empty_words_count as usize
    }

    // From Go: decompress.go:648
    pub fn make_getter(&self) -> Getter {
        let data = self.data[self.words_start as usize..].to_vec();
        log::debug!(
            "Getter data (first 5 bytes): {:02x?}",
            &data[..data.len().min(5)]
        );
        Getter {
            pattern_dict: self.dict.as_ref(),
            pos_dict: self.pos_dict.as_ref(),
            file_name: self.file_name.clone(),
            data,
            data_p: 0,
            data_bit: 0,
            trace: false,
        }
    }

    pub fn close(mut self) {
        self.f = None;
    }
}

// From Go: decompress.go:537
pub struct Getter<'a> {
    pattern_dict: Option<&'a PatternTable>,
    pos_dict: Option<&'a PosTable>,
    file_name: String,
    data: Vec<u8>,
    pub data_p: u64, // Current position in data
    data_bit: usize, // Current bit position (0..7)
    trace: bool,
}

impl<'a> Getter<'a> {
    // From Go: decompress.go:547-548
    pub fn trace(&mut self, t: bool) {
        self.trace = t;
    }

    pub fn file_name(&self) -> &str {
        &self.file_name
    }

    // From Go: decompress.go:550
    fn next_pos(&mut self, clean: bool) -> u64 {
        log::debug!(
            "next_pos called, clean: {}, data_p: {}, data_bit: {}",
            clean,
            self.data_p,
            self.data_bit
        );
        if clean && self.data_bit > 0 {
            self.data_p += 1;
            self.data_bit = 0;
        }

        let table = match self.pos_dict {
            Some(t) => t,
            None => {
                // No position dictionary - read varint directly from data
                if self.data_p >= self.data.len() as u64 {
                    return 0;
                }
                let (pos, size) =
                    decode_varint(&self.data[self.data_p as usize..]).unwrap_or((0, 0));
                self.data_p += size as u64;
                return pos;
            }
        };

        if table.bit_len == 0 {
            log::debug!("next_pos: table.bit_len is 0");
            // Empty position table - read varint directly from data
            if table.pos.is_empty() || (table.pos.len() == 1 && table.pos[0] == 0) {
                log::debug!("next_pos: reading varint from data");
                if self.data_p >= self.data.len() as u64 {
                    return 0;
                }
                let (pos, size) =
                    decode_varint(&self.data[self.data_p as usize..]).unwrap_or((0, 0));
                self.data_p += size as u64;
                return pos;
            }
            return table.pos[0];
        }

        let mut current_table = table;
        log::debug!(
            "next_pos: entering main loop with bit_len: {}",
            current_table.bit_len
        );
        loop {
            let mut code = (self.data[self.data_p as usize] >> self.data_bit) as u16;
            if 8 - self.data_bit < current_table.bit_len
                && (self.data_p as usize) + 1 < self.data.len()
            {
                code |= (self.data[self.data_p as usize + 1] as u16) << (8 - self.data_bit);
            }
            code &= (1u16 << current_table.bit_len) - 1;

            log::debug!(
                "next_pos: code: {}, data_p: {}, data_bit: {}",
                code,
                self.data_p,
                self.data_bit
            );

            let l = current_table.lens[code as usize];
            log::debug!("next_pos: lens[{}] = {}", code, l);
            if l == 0 {
                // Navigate to deeper table
                if let Some(ref next_table) = current_table.ptrs[code as usize] {
                    log::debug!("next_pos: navigating to deeper table");
                    current_table = next_table;
                    self.data_bit += 9;
                } else {
                    log::debug!("next_pos: no deeper table, returning 0");
                    return 0;
                }
            } else {
                self.data_bit += l as usize;
                let pos = current_table.pos[code as usize];
                self.data_p += (self.data_bit / 8) as u64;
                self.data_bit %= 8;
                log::debug!("next_pos returning position: {}", pos);
                return pos;
            }

            self.data_p += (self.data_bit / 8) as u64;
            self.data_bit %= 8;
        }
    }

    // From Go: decompress.go:584
    fn next_pattern(&mut self) -> Vec<u8> {
        let table = match self.pattern_dict {
            Some(t) => t,
            None => return Vec::new(),
        };

        if table.bit_len == 0 {
            return table.patterns[0]
                .as_ref()
                .map(|cw| cw.pattern.clone())
                .unwrap_or_default();
        }

        let mut current_table = table;
        loop {
            let mut code = (self.data[self.data_p as usize] >> self.data_bit) as u16;
            if 8 - self.data_bit < current_table.bit_len
                && (self.data_p as usize) + 1 < self.data.len()
            {
                code |= (self.data[self.data_p as usize + 1] as u16) << (8 - self.data_bit);
            }
            code &= (1u16 << current_table.bit_len) - 1;

            if let Some(cw) = current_table.condensed_table_search(code) {
                let l = cw.len;
                if l == 0 {
                    if let Some(ref ptr) = cw.ptr {
                        current_table = ptr;
                        self.data_bit += 9;
                    } else {
                        return Vec::new();
                    }
                } else {
                    self.data_bit += l as usize;
                    let pattern = cw.pattern.clone();
                    self.data_p += (self.data_bit / 8) as u64;
                    self.data_bit %= 8;
                    return pattern;
                }
            } else {
                return Vec::new();
            }

            self.data_p += (self.data_bit / 8) as u64;
            self.data_bit %= 8;
        }
    }

    // From Go: decompress.go:657
    pub fn reset(&mut self, offset: u64) {
        self.data_p = offset;
        self.data_bit = 0;
    }

    // From Go: decompress.go:662
    pub fn has_next(&self) -> bool {
        self.data_p < self.data.len() as u64
    }

    // From Go: decompress.go:669
    pub fn next(&mut self, mut buf: Vec<u8>) -> (Vec<u8>, u64) {
        log::debug!(
            "Getter::next called, data_p: {}, data_len: {}",
            self.data_p,
            self.data.len()
        );
        let save_pos = self.data_p;
        let mut word_len = self.next_pos(true);

        if word_len > 0 {
            word_len -= 1; // because when creating huffman tree we do ++, because 0 is terminator
        }

        if word_len == 0 {
            if self.data_bit > 0 {
                self.data_p += 1;
                self.data_bit = 0;
            }
            // Empty word
            return (buf, self.data_p);
        }

        let buf_offset = buf.len();
        buf.resize(buf_offset + word_len as usize, 0);

        // First pass: fill in the patterns
        let mut buf_pos = buf_offset;
        let mut pos = self.next_pos(false);
        while pos != 0 {
            buf_pos += pos as usize - 1;
            let pattern = self.next_pattern();
            buf[buf_pos..buf_pos + pattern.len()].copy_from_slice(&pattern);
            pos = self.next_pos(false);
        }

        if self.data_bit > 0 {
            self.data_p += 1;
            self.data_bit = 0;
        }
        let post_loop_pos = self.data_p;

        // Reset to read positions again
        self.data_p = save_pos;
        self.data_bit = 0;
        self.next_pos(true); // Reset the state

        // Second pass: fill in uncovered data
        buf_pos = buf_offset;
        let mut last_uncovered = buf_offset;
        pos = self.next_pos(false);
        while pos != 0 {
            buf_pos += pos as usize - 1;
            if buf_pos > last_uncovered {
                let dif = buf_pos - last_uncovered;
                buf[last_uncovered..buf_pos].copy_from_slice(
                    &self.data[post_loop_pos as usize..post_loop_pos as usize + dif],
                );
            }
            last_uncovered = buf_pos + self.next_pattern().len();
            pos = self.next_pos(false);
        }

        // Fill any remaining uncovered data
        log::debug!(
            "buf_offset: {}, word_len: {}, last_uncovered: {}",
            buf_offset,
            word_len,
            last_uncovered
        );
        if buf_offset + word_len as usize > last_uncovered {
            let dif = buf_offset + word_len as usize - last_uncovered;
            log::debug!(
                "Copying {} uncovered bytes from position {}",
                dif,
                post_loop_pos
            );
            if post_loop_pos as usize + dif <= self.data.len() {
                let mut final_data = vec![0u8; dif];
                final_data.copy_from_slice(
                    &self.data[post_loop_pos as usize..post_loop_pos as usize + dif],
                );
                buf[last_uncovered..last_uncovered + dif].copy_from_slice(&final_data);
            } else {
                log::error!(
                    "Not enough data: need {} bytes at pos {}, but data len is {}",
                    dif,
                    post_loop_pos,
                    self.data.len()
                );
            }
        }

        self.data_p = post_loop_pos + (buf_offset + word_len as usize - last_uncovered) as u64;
        self.data_bit = 0;

        (buf, self.data_p)
    }

    // From Go: decompress.go:738-788
    pub fn match_prefix(&self, prefix: &[u8]) -> bool {
        if prefix.is_empty() {
            return true;
        }

        // Create a temporary getter to peek at the next word
        let mut temp_getter = Getter {
            pattern_dict: self.pattern_dict,
            pos_dict: self.pos_dict,
            file_name: self.file_name.clone(),
            data: self.data.clone(),
            data_p: self.data_p,
            data_bit: self.data_bit,
            trace: false,
        };

        // Decompress the next word to check prefix
        let (word, _) = temp_getter.next(Vec::new());

        // Check if word starts with prefix
        word.starts_with(prefix)
    }

    // From Go: decompress.go:800
    pub fn skip(&mut self) -> u64 {
        let save_pos = self.data_p;
        let mut word_len = self.next_pos(true);

        if word_len > 0 {
            word_len -= 1;
        }

        if word_len == 0 {
            if self.data_bit > 0 {
                self.data_p += 1;
                self.data_bit = 0;
            }
            return self.data_p;
        }

        // Skip patterns
        let mut pos = self.next_pos(false);
        while pos != 0 {
            self.next_pattern();
            pos = self.next_pos(false);
        }

        if self.data_bit > 0 {
            self.data_p += 1;
            self.data_bit = 0;
        }
        let post_loop_pos = self.data_p;

        // Reset and skip positions to calculate uncovered length
        self.data_p = save_pos;
        self.data_bit = 0;
        self.next_pos(true);

        let mut buf_pos = 0;
        let mut last_uncovered = 0;
        pos = self.next_pos(false);
        while pos != 0 {
            buf_pos += pos as usize - 1;
            if buf_pos > last_uncovered {
                // Skip uncovered bytes
            }
            last_uncovered = buf_pos + self.next_pattern().len();
            pos = self.next_pos(false);
        }

        // Calculate final position
        let skip_bytes = if word_len as usize > last_uncovered {
            word_len as usize - last_uncovered
        } else {
            0
        };

        self.data_p = post_loop_pos + skip_bytes as u64;
        self.data_bit = 0;

        self.data_p
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }
}

// From Go: decompress.go:615-636
fn check_distance(power: usize, d: usize) -> bool {
    lazy_static::lazy_static! {
        static ref CONDENSED_WORD_DISTANCES: Vec<Vec<usize>> = build_condensed_word_distances();
    }

    if power >= CONDENSED_WORD_DISTANCES.len() {
        return false;
    }

    CONDENSED_WORD_DISTANCES[power].contains(&d)
}

fn build_condensed_word_distances() -> Vec<Vec<usize>> {
    let mut dist2 = vec![Vec::new(); 10];
    for i in 1..=9 {
        let mut dl = Vec::new();
        let mut j = 1 << i;
        while j < 512 {
            dl.push(j);
            j += 1 << i;
        }
        dist2[i] = dl;
    }
    dist2
}

// Parse pattern dictionary from compressed file data
// From Go: decompress.go:236-275
fn parse_pattern_dictionary(data: &[u8]) -> Result<(PatternTable, usize), CompressionError> {
    let mut dict_pos = 0usize;
    let dict_size = data.len();

    let mut depths = Vec::new();
    let mut patterns = Vec::new();
    let mut pattern_max_depth = 0u64;

    // Read patterns from dictionary
    // Go: decompress.go:243-277
    while dict_pos < dict_size {
        let (depth, ns) = decode_varint(&data[dict_pos..])?;
        if depth > MAX_ALLOWED_DEPTH {
            return Err(CompressionError::Other(format!(
                "Pattern depth {} exceeds maximum allowed depth {}",
                depth, MAX_ALLOWED_DEPTH
            )));
        }

        if depth > pattern_max_depth {
            pattern_max_depth = depth;
        }
        dict_pos += ns;

        let (pattern_size, ns) = decode_varint(&data[dict_pos..])?;
        dict_pos += ns;

        if dict_pos + pattern_size as usize > dict_size {
            return Err(CompressionError::Other(
                "Pattern size exceeds dictionary bounds".to_string(),
            ));
        }

        let pattern = data[dict_pos..dict_pos + pattern_size as usize].to_vec();
        dict_pos += pattern_size as usize;

        depths.push(depth);
        patterns.push(pattern);
    }

    // Allow empty dictionary for minimal compressed files
    // In this case, return empty table
    if patterns.is_empty() && depths.is_empty() {
        let empty_pattern_table = PatternTable::new(0);
        return Ok((empty_pattern_table, 0));
    }

    // Build pattern huffman tree
    // Go: decompress.go:285-361
    let mut pattern_huffs = Vec::new();
    let mut i = 0;
    let patterns_count = patterns.len();

    while i < patterns_count {
        let depth = depths[i];

        if depth == 0 {
            i += 1;
            continue;
        }

        let mut patterns_at_depth = Vec::new();
        while i < patterns_count && depths[i] == depth {
            patterns_at_depth.push(patterns[i].clone());
            i += 1;
        }

        pattern_huffs.push((depth, patterns_at_depth));
    }

    // Build pattern table from huffman data
    let dict = build_pattern_table(&pattern_huffs)?;

    Ok((dict, patterns.len()))
}

// Parse position dictionary from compressed file data
// From Go: decompress.go:282-332
fn parse_position_dictionary(data: &[u8]) -> Result<PosTable, CompressionError> {
    let mut dict_pos = 0usize;
    let dict_size = data.len();

    let mut pos_depths = Vec::new();
    let mut positions = Vec::new();
    let mut pos_max_depth = 0u64;

    // Read positions from dictionary
    while dict_pos < dict_size {
        let (depth, ns) = decode_varint(&data[dict_pos..])?;
        if depth > MAX_ALLOWED_DEPTH {
            return Err(CompressionError::Other(format!(
                "Position depth {} exceeds maximum allowed depth {}",
                depth, MAX_ALLOWED_DEPTH
            )));
        }

        if depth > pos_max_depth {
            pos_max_depth = depth;
        }
        dict_pos += ns;

        let (pos, ns) = decode_varint(&data[dict_pos..])?;
        dict_pos += ns;

        pos_depths.push(depth);
        positions.push(pos);
    }

    // Allow empty position dictionary
    if positions.is_empty() && pos_depths.is_empty() {
        let mut empty_pos_table = PosTable::new(0);
        // For empty dictionary, pos[0] should be 0
        if !empty_pos_table.pos.is_empty() {
            empty_pos_table.pos[0] = 0;
        }
        return Ok(empty_pos_table);
    }

    // Build position huffman tree
    let mut pos_huffs = Vec::new();
    let mut i = 0;
    let positions_count = positions.len();

    log::debug!(
        "Parsing position dictionary: {} positions, max_depth={}",
        positions_count,
        pos_max_depth
    );
    for j in 0..positions_count {
        log::debug!(
            "  Position[{}]: depth={}, pos={}",
            j,
            pos_depths[j],
            positions[j]
        );
    }

    while i < positions_count {
        let depth = pos_depths[i];

        if depth == 0 {
            i += 1;
            continue;
        }

        let mut pos_at_depth = Vec::new();
        while i < positions_count && pos_depths[i] == depth {
            pos_at_depth.push(positions[i]);
            i += 1;
        }

        pos_huffs.push((depth, pos_at_depth));
    }

    log::debug!("Position huffs grouped by depth:");
    for (depth, positions) in &pos_huffs {
        log::debug!("  Depth {}: {:?}", depth, positions);
    }

    // Build position table
    let pos_dict = build_pos_table(&pos_huffs, pos_max_depth)?;

    Ok(pos_dict)
}

// Build pattern table from huffman tree data
fn build_pattern_table(huffs: &[(u64, Vec<Vec<u8>>)]) -> Result<PatternTable, CompressionError> {
    // Find the maximum depth to determine bit_len
    let max_depth = huffs.iter().map(|(depth, _)| *depth).max().unwrap_or(0);
    
    // Use maximum depth as bit_len, with minimum of 9  
    let bit_len = (max_depth as usize).max(9);
    
    // Calculate total patterns to estimate maximum code
    let total_patterns: usize = huffs.iter().map(|(_, patterns)| patterns.len()).sum();
    
    // Use larger bit_len to accommodate potential code spreading
    let safe_bit_len = if total_patterns > 0 {
        // Ensure we have enough space for code + (1 << bit_len)
        let max_code_estimate = total_patterns;
        let required_bits = ((max_code_estimate + (1 << bit_len)) as f64).log2().ceil() as usize;
        required_bits.max(bit_len)
    } else {
        bit_len
    };
    
    log::debug!("Building pattern table: max_depth={}, bit_len={}, safe_bit_len={}, table_size={}", 
        max_depth, bit_len, safe_bit_len, 1 << safe_bit_len);
    
    let mut table = PatternTable::new(safe_bit_len);

    // Build huffman codes for each depth level
    let mut code = 0u16;

    for (depth, patterns) in huffs {
        for pattern in patterns {
            let cw = Codeword {
                pattern: pattern.clone(),
                ptr: None,
                code,
                len: *depth as u8,
            };
            log::debug!("Inserting pattern code={}, len={}, pattern={:?}", 
                code, *depth, String::from_utf8_lossy(pattern));
            table.insert_word(cw);
            code += 1;
        }
    }

    Ok(table)
}

// Build position table from huffman tree data using Go's recursive approach
fn build_pos_table(
    huffs: &[(u64, Vec<u64>)],
    max_depth: u64,
) -> Result<PosTable, CompressionError> {
    // Use max_depth as bit_len, with minimum of 9 (matching Go implementation)
    // From Go: decompress.go:316-320
    let bit_len = if max_depth > 9 { 9 } else { max_depth as usize };

    let mut table = PosTable::new(bit_len);

    // Flatten huffs back to parallel arrays like Go
    let mut pos_depths = Vec::new();
    let mut positions = Vec::new();

    for (depth, pos_list) in huffs {
        for &pos in pos_list {
            pos_depths.push(*depth);
            positions.push(pos);
        }
    }

    // Call recursive build like Go does
    build_pos_table_recursive(&pos_depths, &positions, &mut table, 0, 0, 0, max_depth)?;
    Ok(table)
}

// Recursive position table builder (matching Go's buildPosTable exactly)
fn build_pos_table_recursive(
    depths: &[u64],
    positions: &[u64],
    table: &mut PosTable,
    code: u16,
    bits: u8,
    depth: u64,
    max_depth: u64,
) -> Result<usize, CompressionError> {
    if depths.is_empty() {
        return Ok(0);
    }

    if depth == depths[0] {
        let pos = positions[0];
        if table.bit_len == bits as usize {
            table.pos[code as usize] = pos;
            table.lens[code as usize] = bits;
        } else {
            let code_step = 1u16 << bits;
            let code_from = code;
            let code_to = code | (1u16 << table.bit_len);
            let mut c = code_from;
            while c < code_to {
                table.pos[c as usize] = pos;
                table.lens[c as usize] = bits;
                c += code_step;
            }
        }
        log::debug!("  Table[code={}] = pos={}, len={}", code, pos, bits);
        return Ok(1);
    }

    // Recursive split like Go
    let b0 = build_pos_table_recursive(
        depths,
        positions,
        table,
        code,
        bits + 1,
        depth + 1,
        max_depth - 1,
    )?;
    let b1 = build_pos_table_recursive(
        &depths[b0..],
        &positions[b0..],
        table,
        (1u16 << bits) | code,
        bits + 1,
        depth + 1,
        max_depth - 1,
    )?;
    Ok(b0 + b1)
}

// Decode a varint from bytes
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pattern_table() {
        let mut table = PatternTable::new(4);
        let cw = Codeword {
            pattern: b"test".to_vec(),
            ptr: None,
            code: 5,
            len: 3,
        };
        table.insert_word(cw);

        assert!(table.condensed_table_search(5).is_some());
    }

    #[test]
    fn test_varint_decode() {
        let data = vec![0x96, 0x01]; // 150 in varint
        let (value, size) = decode_varint(&data).unwrap();
        assert_eq!(value, 150);
        assert_eq!(size, 2);
    }

    #[test]
    fn test_condensed_distances() {
        assert!(check_distance(3, 8)); // 1 << 3 = 8
        assert!(check_distance(4, 16)); // 1 << 4 = 16
        assert!(!check_distance(3, 7)); // Not a valid distance
    }
}
