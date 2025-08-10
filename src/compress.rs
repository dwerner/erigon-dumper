// Port of Erigon's compress.go
// Original: go/src/compress.go

use crate::error::CompressionError;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

// From Go: Cfg struct - compression configuration
#[derive(Debug, Clone)]
pub struct Cfg {
    pub min_pattern_score: u64,

    // minPatternLen is minimum length of pattern we consider to be included into the dictionary
    pub min_pattern_len: usize,
    pub max_pattern_len: usize,

    // maxDictPatterns is the maximum number of patterns allowed in the initial (not reduced dictionary)
    // Large values increase memory consumption of dictionary reduction phase
    pub max_dict_patterns: usize,

    // DictReducerSoftLimit - Before creating dict of size MaxDictPatterns - need order patterns by score, but with limited RAM usage.
    pub dict_reducer_soft_limit: usize,

    // samplingFactor - skip superstrings if `superstringNumber % samplingFactor != 0`
    pub sampling_factor: u64,

    pub workers: usize,
}

impl Default for Cfg {
    fn default() -> Self {
        // From Go: DefaultCfg
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

// TODO: missing comment from Go
// From Go: Compressor struct
pub struct Compressor {
    cfg: Cfg,
    output_file: String,
    file_name: String, // File where to output the dictionary and compressed data
    tmp_dir: String,   // temporary directory to use for ETL when building dictionary
    log_prefix: String,

    // From Go: compress.go:105-116
    superstrings: Vec<Vec<u8>>, // Collecting superstrings instead of using channels for now
    uncompressed_file: Option<RawWordsFile>,
    tmp_out_file_path: String,

    // Buffer for "superstring" - transformation where each byte of a word, say b,
    // is turned into 2 bytes, 0x01 and b, and two zero bytes 0x00 0x00 are inserted after each word
    // Go: compress.go:109-113
    superstring: Vec<u8>,
    words_count: u64,
    superstring_count: u64,
    superstring_len: usize,

    // Compression ratio after compression
    ratio: CompressionRatio,
    no_fsync: bool,

    // Go uses sync.WaitGroup, we'll use different synchronization when needed
    // wg: Arc<WaitGroup>,

    // suffixCollectors: Vec<etl::Collector>,
    lvl: log::Level,
    trace: bool,
}

impl Compressor {
    pub fn new(
        cfg: Cfg,
        output_file: String,
        tmp_dir: String,
        log_prefix: String,
        lvl: log::Level,
    ) -> std::result::Result<Self, CompressionError> {
        // Go: compress.go:127-131
        let path = PathBuf::from(&output_file);
        let file_name = path
            .file_name()
            .ok_or_else(|| CompressionError::Other("Invalid output file path".to_string()))?
            .to_string_lossy()
            .to_string();

        // tmpOutFilePath is a ".seg.tmp" file which will be renamed to ".seg" if everything succeeds
        let tmp_out_file_path = format!("{}.tmp", output_file);

        // Create uncompressed file path
        // Go: compress.go:133
        let uncompressed_path = PathBuf::from(&tmp_dir)
            .join(&file_name)
            .with_extension("idt");

        // Go: compress.go:134-137
        let uncompressed_file = RawWordsFile::new(uncompressed_path.to_string_lossy().to_string())?;

        // Note: Using synchronous superstring collection instead of Go's parallel workers/channels
        Ok(Compressor {
            cfg,
            output_file,
            file_name,
            tmp_dir,
            log_prefix,
            superstrings: Vec::new(),
            uncompressed_file: Some(uncompressed_file),
            tmp_out_file_path,
            superstring: Vec::with_capacity(1024 * 1024),
            words_count: 0,
            superstring_count: 0,
            superstring_len: 0,
            ratio: 0.0,
            no_fsync: false,
            lvl,
            trace: lvl <= log::Level::Trace,
        })
    }

    // From Go: compress.go:182
    pub fn count(&self) -> u64 {
        self.words_count
    }

    // From Go: AddWord method - compress.go:195-222
    // REVIEW Q: why is go using a channel here?
    pub fn add_word(&mut self, word: &[u8]) -> std::result::Result<(), CompressionError> {
        self.words_count += 1;

        // Calculate length: 2*len(word) + 2 for the encoding
        let l = 2 * word.len() + 2;

        // Check if we need to start a new superstring
        if self.superstring_len + l > SUPERSTRING_LIMIT {
            // Go: compress.go:205-210
            if self.superstring_count % self.cfg.sampling_factor == 0 {
                // Save current superstring
                let ss = std::mem::replace(&mut self.superstring, Vec::with_capacity(1024 * 1024));
                self.superstrings.push(ss);
            }
            self.superstring_count += 1;
            self.superstring_len = 0;
        }

        self.superstring_len += l;

        // Only add to superstring if we're sampling this one
        // Go: compress.go:214-221
        if self.superstring_count % self.cfg.sampling_factor == 0 {
            for &byte in word {
                self.superstring.push(0x01);
                self.superstring.push(byte);
            }
            self.superstring.push(0x00);
            self.superstring.push(0x00);
        }

        // Also write to uncompressed file like Go does
        // Go: compress.go:221
        if let Some(ref mut file) = self.uncompressed_file {
            file.append(word)?;
        }

        Ok(())
    }

    // From Go: AddUncompressedWord - compress.go:224-233
    pub fn add_uncompressed_word(
        &mut self,
        word: &[u8],
    ) -> std::result::Result<(), CompressionError> {
        self.words_count += 1;

        if let Some(ref mut file) = self.uncompressed_file {
            file.append_uncompressed(word)?;
            Ok(())
        } else {
            Err(CompressionError::Other(
                "Uncompressed file not initialized".to_string(),
            ))
        }
    }

    // From Go: Compress - compress.go:235-292
    pub fn compress(&mut self) -> std::result::Result<(), CompressionError> {
        use std::fs;
        use std::time::Instant;

        let start = Instant::now();

        // Flush uncompressed file
        if let Some(ref mut uf) = self.uncompressed_file {
            uf.flush()?;
        }

        // Add any remaining superstring
        if !self.superstring.is_empty() {
            let ss = std::mem::take(&mut self.superstring);
            self.superstrings.push(ss);
        }

        log::info!("[{}] Building dictionary from {} superstrings", 
                  self.log_prefix, self.superstrings.len());

        // Build dictionary from collected superstrings (synchronous version)
        let dict_builder = self.build_dictionary_from_superstrings()?;

        // Save dictionary for debugging if trace is enabled
        if self.trace {
            let dict_path = PathBuf::from(&self.tmp_dir)
                .join(&self.file_name)
                .with_extension("dictionary.txt");
            persist_dictionary(&dict_path, &dict_builder)?;
        }

        // Create compressed file
        let cf = File::create(&self.tmp_out_file_path).map_err(|e| CompressionError::FileCreate {
            path: self.tmp_out_file_path.clone(),
            source: e,
        })?;

        // Compress with pattern candidates
        if let Some(ref mut uf) = self.uncompressed_file {
            crate::parallel_compress::compress_with_pattern_candidates(
                self.trace,
                &self.cfg,
                &self.log_prefix,
                &self.tmp_out_file_path,
                &mut cf.try_clone()?,
                uf,
                &dict_builder,
            )?;
        }

        // Sync and close file
        self.fsync(&cf)?;
        drop(cf);

        // Rename temp file to final output
        fs::rename(&self.tmp_out_file_path, &self.output_file).map_err(|e| {
            CompressionError::FileRename {
                from: self.tmp_out_file_path.clone(),
                to: self.output_file.clone(),
                source: e,
            }
        })?;

        // Calculate compression ratio
        if let Some(ref uf) = self.uncompressed_file {
            self.ratio = calculate_ratio(&uf.file_path, &self.output_file)?;
        }

        // Log completion
        if self.lvl <= log::Level::Info {
            log::info!(
                "[{}] Compress took {:?}, ratio: {}, file: {}",
                self.log_prefix,
                start.elapsed(),
                ratio_to_string(self.ratio),
                self.file_name
            );
        }

        Ok(())
    }

    // From Go: DisableFsync - compress.go:294
    pub fn disable_fsync(&mut self) {
        self.no_fsync = true;
    }

    // From Go: Ratio getter
    pub fn ratio(&self) -> CompressionRatio {
        self.ratio
    }

    // From Go: fsync - compress.go:299-308
    fn fsync(&self, file: &File) -> std::result::Result<(), CompressionError> {
        if self.no_fsync {
            return Ok(());
        }
        file.sync_all()?;
        Ok(())
    }


    // Build dictionary from superstrings (synchronous version of Go's DictionaryBuilderFromCollectors)
    fn build_dictionary_from_superstrings(&mut self) -> std::result::Result<DictionaryBuilder, CompressionError> {
        // This is the synchronous equivalent of Go's parallel_compress.go:916-947
        
        // Create aggregator to collect patterns from all superstrings
        let mut dict_aggregator = DictAggregator::new();
        
        // Process each superstring to extract patterns (synchronous instead of parallel)
        for superstring in &self.superstrings {
            if superstring.is_empty() {
                continue;
            }
            
            // Extract patterns from this superstring
            let patterns = crate::parallel_compress::extract_patterns_from_single_superstring(
                superstring,
                &self.cfg,
            );
            
            // Add patterns to aggregator
            for pattern in patterns {
                dict_aggregator.process_word(pattern.word, pattern.score)?;
            }
        }
        
        // Finish aggregation
        let collector = dict_aggregator.finish()?;
        
        // Build dictionary from collected patterns
        let mut dict_builder = DictionaryBuilder::new(self.cfg.dict_reducer_soft_limit);
        dict_builder.load_from_collector(collector);
        
        // Apply hard limit
        dict_builder.finish(self.cfg.max_dict_patterns);
        
        // Sort patterns (for compatibility, though heap already maintains order)
        dict_builder.sort();
        
        log::info!("[{}] Dictionary built with {} patterns", 
                  self.log_prefix, dict_builder.len());
        
        Ok(dict_builder)
    }
}

// superstringLimit limits how large can one "superstring" get before it is processed
// CompressorSequential allocates 7 bytes for each uint of superstringLimit. For example,
// superstingLimit 16m will result in 112Mb being allocated for various arrays
// From Go: compress.go:310-313
const SUPERSTRING_LIMIT: usize = 16 * 1024 * 1024;

// ETL buffer sizes from Go's etl package
const ETL_BUFFER_OPTIMAL_SIZE: usize = 256 * 1024 * 1024; // 256MB
const ETL_BUF_IO_SIZE: usize = 64 * 1024; // 64KB

// From Go: DictionaryBuilder struct
pub struct DictionaryBuilder {
    last_word: Vec<u8>,
    items: BinaryHeap<Pattern>, // Using BinaryHeap instead of slice for heap operations
    soft_limit: usize,
    last_word_score: u64,
}

impl DictionaryBuilder {
    pub fn new(soft_limit: usize) -> Self {
        DictionaryBuilder {
            last_word: Vec::new(),
            items: BinaryHeap::new(),
            soft_limit,
            last_word_score: 0,
        }
    }

    // From Go: Reset method
    pub fn reset(&mut self, soft_limit: usize) {
        self.soft_limit = soft_limit;
        self.items.clear();
    }

    // From Go: Len method
    pub fn len(&self) -> usize {
        self.items.len()
    }

    // From Go: processWord method - compress.go:360-366
    pub fn process_word(&mut self, chars: Vec<u8>, score: u64) {
        // Push new pattern to heap
        self.items.push(Pattern::new(chars, score));
        
        // If over soft limit, remove the smallest score element
        if self.items.len() > self.soft_limit {
            // Since BinaryHeap is a max-heap and Pattern's Ord is reversed for min-heap behavior,
            // pop() removes the minimum score element
            self.items.pop();
        }
    }

    // Load patterns from a collector (replaces ETL loadFunc)
    pub fn load_from_collector(&mut self, collector: SimplePatternCollector) {
        for pattern in collector.into_patterns() {
            // Aggregate patterns with same word
            if pattern.word == self.last_word {
                self.last_word_score += pattern.score;
            } else {
                if !self.last_word.is_empty() {
                    self.process_word(self.last_word.clone(), self.last_word_score);
                }
                self.last_word = pattern.word;
                self.last_word_score = pattern.score;
            }
        }
    }

    // From Go: finish method - compress.go:382-390
    pub fn finish(&mut self, hard_limit: usize) {
        // Process the last word if any
        if !self.last_word.is_empty() {
            self.process_word(self.last_word.clone(), self.last_word_score);
            self.last_word.clear();
            self.last_word_score = 0;
        }
        
        // Keep only hard_limit items by removing lowest scores
        while self.items.len() > hard_limit {
            self.items.pop();
        }
    }

    // From Go: ForEach method - compress.go:393-397
    pub fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(u64, &[u8]),
    {
        // Collect all patterns and sort them
        let mut items: Vec<_> = self.items.iter().collect();
        // Sort by score descending (highest score first)
        items.sort_by(|a, b| b.score.cmp(&a.score).then_with(|| a.word.cmp(&b.word)));
        
        // Iterate in sorted order
        for pattern in items {
            f(pattern.score, &pattern.word);
        }
    }
    
    // Get patterns as a vector (for use in compression)
    pub fn into_patterns(self) -> Vec<Pattern> {
        // Convert heap to sorted vector
        let mut patterns: Vec<Pattern> = self.items.into_sorted_vec();
        // Reverse because into_sorted_vec gives us min to max, but we want max to min
        patterns.reverse();
        patterns
    }

    // From Go: Close method - compress.go:399-401
    pub fn close(&mut self) {
        self.items.clear();
        self.last_word.clear();
    }

    // From Go: Sort method - compress.go:345
    pub fn sort(&mut self) {
        // In Go, this sorts the items slice
        // For us, the heap maintains order and for_each/into_patterns handle sorting
        // This is a no-op for compatibility
    }
}


// Pattern represents a byte sequence to be used in compression dictionary
// From Go: Pattern struct
#[derive(Debug, Clone)]
pub struct Pattern {
    pub word: Vec<u8>,        // Pattern characters
    pub score: u64,           // Score assigned to the pattern during dictionary building
    pub uses: u64, // How many times this pattern has been used during search and optimisation
    pub code: u64, // Allocated numerical code (Huffman code after encoding)
    pub code_bits: usize, // Number of bits in the code
    pub depth: usize, // Depth of the pattern in the huffman tree (for encoding in the file)
    pub sequential_code: u64, // Original sequential code (array index) for intermediate file
}

impl Pattern {
    pub fn new(word: Vec<u8>, score: u64) -> Self {
        Pattern {
            word,
            score,
            uses: 0,
            code: 0,
            code_bits: 0,
            depth: 0,
            sequential_code: 0,
        }
    }
}

// PatternList is a slice of patterns that can be sorted
pub type PatternList = Vec<Pattern>;

// From Go: patternListCmp - compress.go:411-419
pub fn pattern_list_cmp(a: &Pattern, b: &Pattern) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match a.uses.cmp(&b.uses) {
        Ordering::Equal => {
            // When uses are equal, compare by reverse of code
            reverse_bits_64(b.code).cmp(&reverse_bits_64(a.code))
        }
        other => other,
    }
}

// Implement ordering for Pattern - used by DictionaryBuilder's BinaryHeap
// This compares by score for selecting top patterns during dictionary building
// For post-Huffman sorting, use pattern_list_cmp explicitly
impl Ord for Pattern {
    fn cmp(&self, other: &Self) -> Ordering {
        // From Go: dictionaryBuilderCmp - compress.go:335-340
        // Reversed for min-heap behavior (BinaryHeap is max-heap by default)
        match other.score.cmp(&self.score) {
            Ordering::Equal => {
                // If scores are equal, compare by word bytes
                self.word.cmp(&other.word)
            }
            ord => ord,
        }
    }
}

// Required for Ord trait in Rust (Go uses comparison functions instead)
impl PartialOrd for Pattern {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// Patterns are equal if both score and word match
impl PartialEq for Pattern {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score && self.word == other.word
    }
}

impl Eq for Pattern {}

// PatternHeap is a min-heap of PatternHuff nodes for building Huffman tree
pub type PatternHeap = std::collections::BinaryHeap<PatternHuffWrapper>;

// Wrapper for PatternHuff to implement Ord for BinaryHeap
pub struct PatternHuffWrapper {
    pub inner: Box<PatternHuff>,
}

impl Ord for PatternHuffWrapper {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse order for min-heap (BinaryHeap is max-heap by default)
        // First compare by uses (ascending)
        match other.inner.uses.cmp(&self.inner.uses) {
            std::cmp::Ordering::Equal => {
                // Then by tie_breaker (ascending)
                other.inner.tie_breaker.cmp(&self.inner.tie_breaker)
            }
            ord => ord,
        }
    }
}

impl PartialOrd for PatternHuffWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for PatternHuffWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.inner.uses == other.inner.uses && self.inner.tie_breaker == other.inner.tie_breaker
    }
}

impl Eq for PatternHuffWrapper {}

// From Go: PatternHuff struct (for Huffman tree)
pub struct PatternHuff {
    pub p0: Option<usize>, // Index into patterns array instead of owning the pattern
    pub p1: Option<usize>, // Index into patterns array instead of owning the pattern
    pub h0: Option<Box<PatternHuff>>,
    pub h1: Option<Box<PatternHuff>>,
    pub uses: u64,
    pub tie_breaker: u64,
}

impl PatternHuff {
    // These methods now need access to the patterns array to update codes
    // We'll pass the patterns array when calling these methods
    pub fn add_zero(&mut self, patterns: &mut [Pattern]) {
        if let Some(idx) = self.p0 {
            patterns[idx].code <<= 1;
            patterns[idx].code_bits += 1;
        } else if let Some(ref mut h0) = self.h0 {
            h0.add_zero(patterns);
        }

        if let Some(idx) = self.p1 {
            patterns[idx].code <<= 1;
            patterns[idx].code_bits += 1;
        } else if let Some(ref mut h1) = self.h1 {
            h1.add_zero(patterns);
        }
    }

    pub fn add_one(&mut self, patterns: &mut [Pattern]) {
        if let Some(idx) = self.p0 {
            patterns[idx].code <<= 1;
            patterns[idx].code |= 1;
            patterns[idx].code_bits += 1;
        } else if let Some(ref mut h0) = self.h0 {
            h0.add_one(patterns);
        }

        if let Some(idx) = self.p1 {
            patterns[idx].code <<= 1;
            patterns[idx].code |= 1;
            patterns[idx].code_bits += 1;
        } else if let Some(ref mut h1) = self.h1 {
            h1.add_one(patterns);
        }
    }

    pub fn set_depth(&mut self, depth: usize, patterns: &mut [Pattern]) {
        if let Some(idx) = self.p0 {
            patterns[idx].depth = depth + 1;
            patterns[idx].uses = 0;
        }
        if let Some(idx) = self.p1 {
            patterns[idx].depth = depth + 1;
            patterns[idx].uses = 0;
        }
        if let Some(ref mut h0) = self.h0 {
            h0.set_depth(depth + 1, patterns);
        }
        if let Some(ref mut h1) = self.h1 {
            h1.set_depth(depth + 1, patterns);
        }
    }
}

// From Go: Position struct (from compress.go)
#[derive(Debug, Clone)]
pub struct Position {
    pub uses: u64,
    pub pos: u64,
    pub code: u64,
    pub code_bits: usize,
    pub depth: usize, // Depth of the position in the huffman tree
}


// From Go: PositionHuff struct (for Huffman tree)
pub struct PositionHuff {
    pub p0: Option<Box<Position>>,
    pub p1: Option<Box<Position>>,
    pub h0: Option<Box<PositionHuff>>,
    pub h1: Option<Box<PositionHuff>>,
    pub uses: u64,
    pub tie_breaker: u64,
}

impl PositionHuff {
    pub fn add_zero(&mut self) {
        if let Some(ref mut p0) = self.p0 {
            p0.code <<= 1;
            p0.code_bits += 1;
        } else if let Some(ref mut h0) = self.h0 {
            h0.add_zero();
        }

        if let Some(ref mut p1) = self.p1 {
            p1.code <<= 1;
            p1.code_bits += 1;
        } else if let Some(ref mut h1) = self.h1 {
            h1.add_zero();
        }
    }

    pub fn add_one(&mut self) {
        if let Some(ref mut p0) = self.p0 {
            p0.code <<= 1;
            p0.code |= 1;
            p0.code_bits += 1;
        } else if let Some(ref mut h0) = self.h0 {
            h0.add_one();
        }

        if let Some(ref mut p1) = self.p1 {
            p1.code <<= 1;
            p1.code |= 1;
            p1.code_bits += 1;
        } else if let Some(ref mut h1) = self.h1 {
            h1.add_one();
        }
    }

    pub fn set_depth(&mut self, depth: usize) {
        if let Some(ref mut p0) = self.p0 {
            p0.depth = depth + 1;
            p0.uses = 0;
        }
        if let Some(ref mut p1) = self.p1 {
            p1.depth = depth + 1;
            p1.uses = 0;
        }
        if let Some(ref mut h0) = self.h0 {
            h0.set_depth(depth + 1);
        }
        if let Some(ref mut h1) = self.h1 {
            h1.set_depth(depth + 1);
        }
    }
}

// PositionList is a slice of positions that can be sorted
pub type PositionList = Vec<Position>;

// From Go: positionListCmp - parallel_compress.go:429-434
pub fn position_list_cmp(a: &Position, b: &Position) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match a.uses.cmp(&b.uses) {
        Ordering::Equal => {
            // When uses are equal, compare by reverse of code
            reverse_bits_64(b.code).cmp(&reverse_bits_64(a.code))
        }
        other => other,
    }
}

// PositionHeap is a min-heap of PositionHuff nodes for building Huffman tree
pub type PositionHeap = std::collections::BinaryHeap<PositionHuffWrapper>;

// Wrapper for PositionHuff to implement Ord for BinaryHeap
pub struct PositionHuffWrapper {
    pub inner: Box<PositionHuff>,
}

impl Ord for PositionHuffWrapper {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse order for min-heap
        match other.inner.uses.cmp(&self.inner.uses) {
            std::cmp::Ordering::Equal => {
                other.inner.tie_breaker.cmp(&self.inner.tie_breaker)
            }
            ord => ord,
        }
    }
}

impl PartialOrd for PositionHuffWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for PositionHuffWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.inner.uses == other.inner.uses && self.inner.tie_breaker == other.inner.tie_breaker
    }
}

impl Eq for PositionHuffWrapper {}

// From Go: BitWriter struct (from compress.go)
pub struct BitWriter {
    w: BufWriter<File>,
    output_bits: usize,
    output_byte: u8,
}

impl BitWriter {
    pub fn new(w: BufWriter<File>) -> Self {
        BitWriter {
            w,
            output_bits: 0,
            output_byte: 0,
        }
    }

    // From Go: encode method
    pub fn encode(
        &mut self,
        mut code: u64,
        mut code_bits: usize,
    ) -> std::result::Result<(), std::io::Error> {
        // Go: compress.go:642-659
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

    // From Go: flush method - compress.go:661-665
    pub fn flush(&mut self) -> std::result::Result<(), std::io::Error> {
        if self.output_bits > 0 {
            self.w.write_all(&[self.output_byte])?;
            self.output_bits = 0;
            self.output_byte = 0;
        }
        self.w.flush()?;
        Ok(())
    }
}

// From Go: DynamicCell struct (from compress.go)
#[derive(Debug, Clone, Copy)]
pub struct DynamicCell {
    pub optim_start: usize,
    pub cover_start: usize,
    pub compression: i32, // Changed to i32 to match Go's int type
    pub score: u64,
    pub pattern_idx: usize, // offset of the last element in the pattern slice
}

// From Go: Ring struct (from compress.go)
pub struct Ring {
    cells: Vec<DynamicCell>,
    head: usize,
    tail: usize,
    count: usize,
}

impl Ring {
    pub fn new() -> Self {
        // Go: compress.go:691-697
        Ring {
            cells: vec![
                DynamicCell {
                    optim_start: 0,
                    cover_start: 0,
                    compression: 0,
                    score: 0,
                    pattern_idx: 0,
                };
                16
            ],
            head: 0,
            tail: 0,
            count: 0,
        }
    }

    // From Go: Reset - compress.go:700-704
    pub fn reset(&mut self) {
        self.count = 0;
        self.head = 0;
        self.tail = 0;
    }

    // From Go: ensureSize - compress.go:706-720
    fn ensure_size(&mut self) {
        if self.count < self.cells.len() {
            return;
        }
        let mut new_cells = vec![
            DynamicCell {
                optim_start: 0,
                cover_start: 0,
                compression: 0,
                score: 0,
                pattern_idx: 0,
            };
            self.count * 2
        ];
        if self.tail > self.head {
            new_cells[..self.tail - self.head].copy_from_slice(&self.cells[self.head..self.tail]);
        } else {
            let n = self.cells.len() - self.head;
            new_cells[..n].copy_from_slice(&self.cells[self.head..]);
            new_cells[n..n + self.tail].copy_from_slice(&self.cells[..self.tail]);
        }
        self.head = 0;
        self.tail = self.count;
        self.cells = new_cells;
    }

    // From Go: PushFront - compress.go:722-730
    pub fn push_front(&mut self) -> &mut DynamicCell {
        self.ensure_size();
        if self.head == 0 {
            self.head = self.cells.len();
        }
        self.head -= 1;
        self.count += 1;
        &mut self.cells[self.head]
    }

    // From Go: PushBack - compress.go:732-741
    pub fn push_back(&mut self) -> &mut DynamicCell {
        self.ensure_size();
        let idx = self.tail;
        if self.tail == self.cells.len() - 1 {
            self.tail = 0;
        } else {
            self.tail += 1;
        }
        self.count += 1;
        &mut self.cells[idx]
    }

    // From Go: Len - compress.go:743-745
    pub fn len(&self) -> usize {
        self.count
    }

    // From Go: Get - compress.go:747-752
    pub fn get(&mut self, i: usize) -> &mut DynamicCell {
        if i >= self.count {
            panic!("Ring::get index out of bounds");
        }
        let idx = (self.head + i) % self.cells.len();
        &mut self.cells[idx]
    }

    // From Go: Truncate - compress.go:754-758
    pub fn truncate(&mut self, i: usize) {
        self.count = i;
        self.tail = (self.head + i) % self.cells.len();
    }
}

// Simple ETL collector substitute - synchronous pattern collector
// This replaces Go's ETL collector system for dictionary building
pub struct SimplePatternCollector {
    patterns: std::collections::HashMap<Vec<u8>, u64>, // pattern -> accumulated score
    total_patterns: usize,
}

impl SimplePatternCollector {
    pub fn new() -> Self {
        SimplePatternCollector {
            patterns: std::collections::HashMap::new(),
            total_patterns: 0,
        }
    }

    // Add a pattern with its score (accumulates if pattern exists)
    pub fn collect(&mut self, pattern: Vec<u8>, score: u64) {
        *self.patterns.entry(pattern).or_insert(0) += score;
        self.total_patterns += 1;
    }

    // Convert to sorted list of patterns
    pub fn into_patterns(self) -> Vec<Pattern> {
        self.patterns
            .into_iter()
            .map(|(word, score)| Pattern::new(word, score))
            .collect()
    }

    pub fn len(&self) -> usize {
        self.patterns.len()
    }
}

// From Go: DictAggregator struct
pub struct DictAggregator {
    collector: SimplePatternCollector,
    dist: std::collections::HashMap<usize, usize>,
    received_words: usize,
    last_word: Vec<u8>,
    last_word_score: u64,
}

impl DictAggregator {
    pub fn new() -> Self {
        DictAggregator {
            collector: SimplePatternCollector::new(),
            dist: std::collections::HashMap::new(),
            received_words: 0,
            last_word: Vec::new(),
            last_word_score: 0,
        }
    }

    // From Go: processWord method - compress.go:768-773
    pub fn process_word(
        &mut self,
        word: Vec<u8>,
        score: u64,
    ) -> std::result::Result<(), CompressionError> {
        self.received_words += 1;
        *self.dist.entry(word.len()).or_insert(0) += 1;
        
        // Accumulate patterns with same word
        if word == self.last_word {
            self.last_word_score += score;
        } else {
            if !self.last_word.is_empty() {
                self.collector.collect(self.last_word.clone(), self.last_word_score);
            }
            self.last_word = word;
            self.last_word_score = score;
        }
        Ok(())
    }
    
    // Finish aggregation and return collector
    pub fn finish(mut self) -> std::result::Result<SimplePatternCollector, CompressionError> {
        // Process the last word if any
        if !self.last_word.is_empty() {
            self.collector.collect(self.last_word, self.last_word_score);
        }
        
        log::debug!("DictAggregator: processed {} words into {} unique patterns", 
                   self.received_words, self.collector.len());
        
        Ok(self.collector)
    }
}

// From Go: CompressionRatio type
pub type CompressionRatio = f64;

// Helper function to format compression ratio
pub fn ratio_to_string(ratio: CompressionRatio) -> String {
    format!("{:.2}", ratio)
}

// RawWordsFile represents a file with raw (uncompressed) words
// Format: [varint_length][word_bytes]... where varint_length's LSB indicates compression
// From Go: RawWordsFile struct
pub struct RawWordsFile {
    f: File,
    w: BufWriter<File>,
    pub file_path: String,
    buf: [u8; 128], // Buffer for varint encoding - matches Go's 128 byte buffer
    pub count: u64,
}

// From Go: OpenRawWordsFile - compress.go:824-832
pub fn open_raw_words_file(file_path: String) -> std::result::Result<RawWordsFile, CompressionError> {
    use std::fs::OpenOptions;
    let f = OpenOptions::new()
        .read(true)
        .write(false)
        .open(&file_path)
        .map_err(|e| CompressionError::FileOpen {
            path: file_path.clone(),
            source: e,
        })?;
    let w = BufWriter::new(f.try_clone()?);
    
    Ok(RawWordsFile {
        f,
        w,
        file_path,
        buf: [0; 128],
        count: 0,
    })
}

impl RawWordsFile {
    pub fn new(file_path: String) -> std::result::Result<Self, CompressionError> {
        // Go: compress.go:833-841
        // Open with read-write permissions so we can read back later
        use std::fs::OpenOptions;
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&file_path)
            .map_err(|e| CompressionError::FileCreate {
                path: file_path.clone(),
                source: e,
            })?;
        let w = BufWriter::new(f.try_clone()?);

        Ok(RawWordsFile {
            f,
            w,
            file_path,
            buf: [0; 128], // Match Go's buffer size
            count: 0,
        })
    }

    // From Go: Append - compress.go:860-872
    pub fn append(&mut self, v: &[u8]) -> std::result::Result<(), CompressionError> {
        self.count += 1;
        // For compressed words, the length prefix is shifted to make lowest bit zero
        let n = encode_varint(&mut self.buf, 2 * v.len() as u64);
        self.w.write_all(&self.buf[..n])?;
        if !v.is_empty() {
            self.w.write_all(v)?;
        }
        Ok(())
    }

    // From Go: AppendUncompressed - compress.go:874-887
    pub fn append_uncompressed(&mut self, v: &[u8]) -> std::result::Result<(), CompressionError> {
        self.count += 1;
        // For uncompressed words, the length prefix is shifted to make lowest bit one
        let n = encode_varint(&mut self.buf, 2 * v.len() as u64 + 1);
        self.w.write_all(&self.buf[..n])?;
        if !v.is_empty() {
            self.w.write_all(v)?;
        }
        Ok(())
    }

    // From Go: Flush - compress.go:849-851
    pub fn flush(&mut self) -> std::result::Result<(), CompressionError> {
        self.w.flush()?;
        Ok(())
    }

    // From Go: Close - compress.go:852-855
    pub fn close(mut self) -> std::result::Result<(), CompressionError> {
        self.w.flush()?;
        // File is closed when dropped
        Ok(())
    }

    // From Go: CloseAndRemove - compress.go:856-859
    pub fn close_and_remove(mut self) -> std::result::Result<(), CompressionError> {
        self.w.flush()?;
        drop(self.w);
        drop(self.f);
        std::fs::remove_file(&self.file_path)?;
        Ok(())
    }

    // From Go: ForEach - compress.go:889-917
    pub fn for_each<F>(&mut self, mut walker: F) -> std::result::Result<(), CompressionError>
    where
        F: FnMut(&[u8], bool) -> std::result::Result<(), CompressionError>,
    {
        use std::io::{BufReader, Read, Seek};

        // Seek to beginning
        self.f.seek(std::io::SeekFrom::Start(0))?;
        log::debug!(
            "RawWordsFile::for_each - starting at position 0, count: {}",
            self.count
        );

        // Use 8MB buffer like Go does
        const BUF_SIZE: usize = 8 * 1024 * 1024;
        let mut reader = BufReader::with_capacity(BUF_SIZE, &self.f);
        let mut buf = vec![0u8; 16 * 1024];

        loop {
            // Read varint length using our helper function
            let mut l = match read_uvarint(&mut reader) {
                Ok(val) => val,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    log::debug!("RawWordsFile::for_each - EOF reached");
                    return Ok(());
                }
                Err(e) => {
                    log::debug!("RawWordsFile::for_each - Error reading varint: {:?}", e);
                    return Err(CompressionError::from(e));
                }
            };

            // Extract lowest bit as "uncompressed" flag
            let compressed = (l & 1) == 0;
            l >>= 1;

            // Read word bytes
            if buf.len() < l as usize {
                buf.resize(l as usize, 0);
            }
            reader.read_exact(&mut buf[..l as usize])?;

            log::debug!(
                "RawWordsFile::for_each - read word of length {}, compressed: {}",
                l,
                compressed
            );
            walker(&buf[..l as usize], compressed)?;
        }
    }
}


// From Go: CompressionWord struct (from parallel_compress.go)
#[derive(Debug, Clone)]
pub struct CompressionWord {
    pub word: Vec<u8>,
    pub order: u64,
}

impl CompressionWord {
    pub fn new(word: Vec<u8>, order: u64) -> Self {
        CompressionWord { word, order }
    }
}


// Helper function to encode varint (like Go's binary.PutUvarint)
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

// Helper function to decode varint (like Go's binary.ReadUvarint)
pub fn read_uvarint(reader: &mut impl std::io::Read) -> std::io::Result<u64> {
    let mut x = 0u64;
    let mut shift = 0;
    loop {
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte)?;
        
        if shift == 63 && byte[0] > 0x01 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "varint overflow"
            ));
        }
        
        if byte[0] < 0x80 {
            return Ok(x | ((byte[0] as u64) << shift));
        }
        
        x |= ((byte[0] & 0x7F) as u64) << shift;
        shift += 7;
    }
}


// Helper functions

// From Go: bits.Reverse64 function
pub fn reverse_bits_64(x: u64) -> u64 {
    x.reverse_bits()
}

// From Go: persistDictionary - compress.go
fn persist_dictionary(
    path: &std::path::Path,
    dict: &DictionaryBuilder,
) -> std::result::Result<(), CompressionError> {
    use std::fs::File;
    use std::io::Write;

    let mut file = File::create(path)?;
    dict.for_each(|score, word| {
        writeln!(file, "{}: {:?}", score, String::from_utf8_lossy(word)).ok();
    });
    Ok(())
}

// From Go: calculateRatio - compress.go
fn calculate_ratio(
    uncompressed_path: &str,
    compressed_path: &str,
) -> std::result::Result<CompressionRatio, CompressionError> {
    use std::fs;

    let uncompressed_meta = fs::metadata(uncompressed_path)?;
    let compressed_meta = fs::metadata(compressed_path)?;

    let uncompressed_size = uncompressed_meta.len();
    let compressed_size = compressed_meta.len();

    log::debug!(
        "Uncompressed size: {}, Compressed size: {}",
        uncompressed_size,
        compressed_size
    );

    if compressed_size == 0 {
        // If compressed file is empty, return 0 ratio
        return Ok(0.0);
    }

    let ratio = uncompressed_size as f64 / compressed_size as f64;
    Ok(ratio)
}

// Include test module
#[cfg(test)]
#[path = "compress_test.rs"]
mod compress_test;
