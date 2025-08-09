// Port of Erigon's compress.go
// Original: go/src/compress.go

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

// From Go: Pattern struct
#[derive(Debug, Clone)]
pub struct Pattern {
    pub word: Vec<u8>,      // Pattern characters
    pub score: u64,          // Score assigned to the pattern during dictionary building
    pub uses: u64,           // How many times this pattern has been used during search and optimisation
    pub code: u64,           // Allocated numerical code
    pub code_bits: usize,   // Number of bits in the code
    pub depth: usize,        // Depth of the pattern in the huffman tree (for encoding in the file)
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
        }
    }
}

// Implement ordering for Pattern to use in BinaryHeap (max-heap by default in Rust)
// Go uses a min-heap, so we need to reverse the ordering
impl Ord for Pattern {
    fn cmp(&self, other: &Self) -> Ordering {
        // From Go: compress.go:328-340
        // First compare by score (reversed for min-heap behavior)
        match other.score.cmp(&self.score) {
            Ordering::Equal => {
                // If scores are equal, compare by word bytes
                // Go: bytes.Compare(db.items[i].word, db.items[j].word) < 0
                self.word.cmp(&other.word)
            }
            ord => ord,
        }
    }
}

impl PartialOrd for Pattern {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Pattern {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score && self.word == other.word
    }
}

impl Eq for Pattern {}

// From Go: DictionaryBuilder struct
pub struct DictionaryBuilder {
    last_word: Vec<u8>,
    items: BinaryHeap<Pattern>,  // Using BinaryHeap instead of slice for heap operations
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

    // From Go: processWord method
    pub fn process_word(&mut self, chars: Vec<u8>, score: u64) {
        // Go: compress.go:360-366
        self.items.push(Pattern::new(chars, score));
        if self.items.len() > self.soft_limit {
            // Pop the smallest element (min score)
            self.items.pop();
        }
    }

    // From Go: loadFunc method - will be implemented when we port ETL
    // pub fn load_func(&mut self, k: &[u8], v: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    //     // Go: compress.go:368-380
    //     unimplemented!()
    // }

    // From Go: finish method
    pub fn finish(&mut self, hard_limit: usize) {
        // Go: compress.go:382-390
        if !self.last_word.is_empty() {
            self.process_word(self.last_word.clone(), self.last_word_score);
        }
        // Keep only hard_limit items
        while self.items.len() > hard_limit {
            self.items.pop();
        }
    }

    // From Go: ForEach method
    pub fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(u64, &[u8]),
    {
        // Go: compress.go:393-397
        // Note: BinaryHeap doesn't provide ordered iteration
        // We need to collect and sort for ordered iteration
        let mut items: Vec<_> = self.items.iter().collect();
        items.sort_by(|a, b| b.cmp(a)); // Sort in descending order by score
        for pattern in items {
            f(pattern.score, &pattern.word);
        }
    }

    // From Go: Close method
    pub fn close(&mut self) {
        // Go: compress.go:399-401
        self.items.clear();
        self.last_word.clear();
    }
}

// From Go: Compressor struct
pub struct Compressor {
    cfg: Cfg,
    output_file: String,
    file_name: String,  // File where to output the dictionary and compressed data
    tmp_dir: String,    // temporary directory to use for ETL when building dictionary
    log_prefix: String,
    
    // Using channels would require async in Rust, we'll use different approach for now
    // superstrings: Option<Sender<Vec<u8>>>,
    // uncompressed_file: Option<RawWordsFile>,
    tmp_out_file_path: String,
    
    // Go uses sync.WaitGroup, we'll use different synchronization when needed
    // wg: Arc<WaitGroup>,
    
    // suffixCollectors: Vec<etl::Collector>,
    lvl: log::Level,
    trace: bool,
}

impl Compressor {
    pub fn new(cfg: Cfg, output_file: String, tmp_dir: String, log_prefix: String, lvl: log::Level) -> Self {
        let file_name = PathBuf::from(&output_file)
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string();
        
        Compressor {
            cfg,
            output_file: output_file.clone(),
            file_name,
            tmp_dir,
            log_prefix,
            tmp_out_file_path: String::new(),
            lvl,
            trace: false,
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

// From Go: DictAggregator struct
pub struct DictAggregator {
    // collector: etl::Collector,  // Will implement ETL later
    dist: std::collections::HashMap<usize, usize>,
    received_words: usize,
    last_word: Vec<u8>,
    last_word_score: u64,
}

impl DictAggregator {
    pub fn new() -> Self {
        DictAggregator {
            dist: std::collections::HashMap::new(),
            received_words: 0,
            last_word: Vec::new(),
            last_word_score: 0,
        }
    }

    // From Go: processWord method
    pub fn process_word(&mut self, word: Vec<u8>, score: u64) -> Result<(), Box<dyn std::error::Error>> {
        // Go: compress.go:768-773
        // Will implement when we have ETL collector
        unimplemented!()
    }
}

// From Go: RawWordsFile struct
pub struct RawWordsFile {
    f: File,
    w: BufWriter<File>,
    file_path: String,
    buf: Vec<u8>,
    count: u64,
}

impl RawWordsFile {
    pub fn new(file_path: String) -> Result<Self, std::io::Error> {
        // Go: compress.go:833-841
        let f = File::create(&file_path)?;
        let w = BufWriter::new(f.try_clone()?);
        
        Ok(RawWordsFile {
            f,
            w,
            file_path,
            buf: Vec::new(),
            count: 0,
        })
    }

    // Will implement Write, Flush, Close methods when needed
}

// From Go: Position struct (from compress.go)
#[derive(Debug, Clone)]
pub struct Position {
    pub uses: u64,
    pub pos: u64,
    pub code: u64,
    pub code_bits: usize,
    pub depth: usize,  // Depth of the position in the huffman tree
}

// From Go: PatternHuff struct (for Huffman tree)
pub struct PatternHuff {
    p0: Option<Box<Pattern>>,
    p1: Option<Box<Pattern>>,
    h0: Option<Box<PatternHuff>>,
    h1: Option<Box<PatternHuff>>,
    uses: u64,
    tie_breaker: u64,
}

// From Go: PositionHuff struct (for Huffman tree)
pub struct PositionHuff {
    p0: Option<Box<Position>>,
    p1: Option<Box<Position>>,
    h0: Option<Box<PositionHuff>>,
    h1: Option<Box<PositionHuff>>,
    uses: u64,
    tie_breaker: u64,
}

// From Go: DynamicCell struct (from compress.go)
#[derive(Debug, Clone)]
pub struct DynamicCell {
    pub optim_start: usize,
    pub cover_start: usize,
    pub compression: usize,
    pub score: u64,
    pub pattern_idx: usize,  // offset of the last element in the pattern slice
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
            cells: vec![DynamicCell {
                optim_start: 0,
                cover_start: 0,
                compression: 0,
                score: 0,
                pattern_idx: 0,
            }; 16],
            head: 0,
            tail: 0,
            count: 0,
        }
    }
    
    // Will implement PushFront, PushBack, Get methods when needed
}

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
    pub fn encode(&mut self, mut code: u64, mut code_bits: usize) -> Result<(), std::io::Error> {
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

    // Will implement flush and other methods when needed
}

// Include test module
#[cfg(test)]
#[path = "compress_test.rs"]
mod compress_test;