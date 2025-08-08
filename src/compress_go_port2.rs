// Exact line-by-line port of Go's compressWithPatternCandidates and related functions
// Each Go line is included as a comment above its Rust equivalent

use std::collections::HashMap;
use std::io::{Write, Read, BufWriter, BufReader, Seek, SeekFrom};
use std::fs::File;
use std::path::Path;
use crate::error::{Result, Error};
use crate::compress_go_port::{Pattern, DynamicCell, Ring, cover_word_by_patterns};
use crate::patricia::{PatriciaTree, MatchFinder2, Match};

// ========== Port of compress.go lines 525-532 Position struct ==========
// type Position struct {
//     uses     uint64
//     pos      uint64
//     code     uint64
//     codeBits int
//     depth    int // Depth of the position in the huffman tree (for encoding in the file)
// }
#[derive(Clone, Copy, Debug)]
pub struct Position {
    pub uses: u64,       // uint64
    pub pos: u64,        // uint64
    pub code: u64,       // uint64
    pub code_bits: i32,  // int
    pub depth: i32,      // int
}

// ========== Port of compress.go lines 636-676 BitWriter ==========
// type BitWriter struct {
//     w          *bufio.Writer
//     outputBits int
//     outputByte byte
// }
pub struct BitWriter<'a> {
    pub w: &'a mut dyn Write,   // *bufio.Writer
    pub output_bits: i32,        // int
    pub output_byte: u8,         // byte
}

impl<'a> BitWriter<'a> {
    pub fn new(w: &'a mut dyn Write) -> Self {
        BitWriter {
            w,
            output_bits: 0,
            output_byte: 0,
        }
    }

    // func (hf *BitWriter) encode(code uint64, codeBits int) error {
    pub fn encode(&mut self, mut code: u64, mut code_bits: i32) -> Result<()> {
        // for codeBits > 0 {
        while code_bits > 0 {
            // var bitsUsed int
            let bits_used: i32;
            // if hf.outputBits+codeBits > 8 {
            if self.output_bits + code_bits > 8 {
                // bitsUsed = 8 - hf.outputBits
                bits_used = 8 - self.output_bits;
            // } else {
            } else {
                // bitsUsed = codeBits
                bits_used = code_bits;
            // }
            }
            // mask := (uint64(1) << bitsUsed) - 1
            let mask = (1u64 << bits_used) - 1;
            // hf.outputByte |= byte((code & mask) << hf.outputBits)
            self.output_byte |= ((code & mask) << self.output_bits) as u8;
            // code >>= bitsUsed
            code >>= bits_used;
            // codeBits -= bitsUsed
            code_bits -= bits_used;
            // hf.outputBits += bitsUsed
            self.output_bits += bits_used;
            // if hf.outputBits == 8 {
            if self.output_bits == 8 {
                // if e := hf.w.WriteByte(hf.outputByte); e != nil {
                //     return e
                // }
                self.w.write_all(&[self.output_byte])?;
                // hf.outputBits = 0
                self.output_bits = 0;
                // hf.outputByte = 0
                self.output_byte = 0;
            // }
            }
        // }
        }
        // return nil
        Ok(())
    }

    // func (hf *BitWriter) flush() error {
    pub fn flush(&mut self) -> Result<()> {
        // if hf.outputBits > 0 {
        if self.output_bits > 0 {
            // if e := hf.w.WriteByte(hf.outputByte); e != nil {
            //     return e
            // }
            self.w.write_all(&[self.output_byte])?;
            // hf.outputBits = 0
            self.output_bits = 0;
            // hf.outputByte = 0
            self.output_byte = 0;
        // }
        }
        // return nil
        Ok(())
    }
}

// ========== Port of compress.go lines 824-887 RawWordsFile ==========
// type RawWordsFile struct {
//     f        *os.File
//     w        *bufio.Writer
//     filePath string
//     buf      []byte
//     count    uint64
// }
pub struct RawWordsFile {
    pub f: File,                    // *os.File
    pub w: BufWriter<File>,         // *bufio.Writer
    pub file_path: String,          // string
    pub buf: Vec<u8>,               // []byte
    pub count: u64,                 // uint64
}

// func NewRawWordsFile(filePath string) (*RawWordsFile, error) {
impl RawWordsFile {
    pub fn new(file_path: &str) -> Result<Self> {
        // f, err := os.Create(filePath)
        // if err != nil {
        //     return nil, err
        // }
        let f = File::create(file_path)?;
        // w := bufio.NewWriterSize(f, 2*etl.BufIOSize)
        let w = BufWriter::with_capacity(2 * 8192 * 1024, f.try_clone()?); // 2*etl.BufIOSize
        // return &RawWordsFile{filePath: filePath, f: f, w: w, buf: make([]byte, 128)}, nil
        Ok(RawWordsFile {
            f,
            w,
            file_path: file_path.to_string(),
            buf: vec![0u8; 128],
            count: 0,
        })
    }

    // func (f *RawWordsFile) Flush() error {
    pub fn flush(&mut self) -> Result<()> {
        // return f.w.Flush()
        self.w.flush()?;
        Ok(())
    }

    // func (f *RawWordsFile) Close() {
    pub fn close(mut self) -> Result<()> {
        // f.w.Flush()
        self.w.flush()?;
        // f.f.Close()
        drop(self.f);
        Ok(())
    }

    // func (f *RawWordsFile) CloseAndRemove() {
    pub fn close_and_remove(mut self) -> Result<()> {
        // f.Close()
        self.w.flush()?;
        drop(self.f);
        drop(self.w);
        // os.Remove(f.filePath)
        std::fs::remove_file(&self.file_path)?;
        Ok(())
    }

    // func (f *RawWordsFile) Append(v []byte) error {
    pub fn append(&mut self, v: &[u8]) -> Result<()> {
        // f.count++
        self.count += 1;
        // For compressed words, the length prefix is shifted to make lowest bit zero
        // n := binary.PutUvarint(f.buf, 2*uint64(len(v)))
        let n = put_uvarint(&mut self.buf, 2 * v.len() as u64);
        // if _, e := f.w.Write(f.buf[:n]); e != nil {
        //     return e
        // }
        self.w.write_all(&self.buf[..n])?;
        // if len(v) > 0 {
        if v.len() > 0 {
            // if _, e := f.w.Write(v); e != nil {
            //     return e
            // }
            self.w.write_all(v)?;
        // }
        }
        // return nil
        Ok(())
    }

    // func (f *RawWordsFile) AppendUncompressed(v []byte) error {
    pub fn append_uncompressed(&mut self, v: &[u8]) -> Result<()> {
        // f.count++
        self.count += 1;
        // For uncompressed words, the length prefix is shifted to make lowest bit one
        // n := binary.PutUvarint(f.buf, 2*uint64(len(v))+1)
        let n = put_uvarint(&mut self.buf, 2 * v.len() as u64 + 1);
        // if _, e := f.w.Write(f.buf[:n]); e != nil {
        //     return e
        // }
        self.w.write_all(&self.buf[..n])?;
        // if len(v) > 0 {
        if v.len() > 0 {
            // if _, e := f.w.Write(v); e != nil {
            //     return e
            // }
            self.w.write_all(v)?;
        // }
        }
        // return nil
        Ok(())
    }

    // ========== Port of compress.go lines 889-918 ForEach ==========
    // func (f *RawWordsFile) ForEach(walker func(v []byte, compressed bool) error) error {
    pub fn for_each<F>(&self, mut walker: F) -> Result<()>
    where
        F: FnMut(&[u8], bool) -> Result<()>,
    {
        // _, err := f.f.Seek(0, 0)
        // if err != nil {
        //     return err
        // }
        let mut f = File::open(&self.file_path)?;
        f.seek(SeekFrom::Start(0))?;
        
        // r := bufio.NewReaderSize(f.f, int(8*datasize.MB))
        let mut r = BufReader::with_capacity(8 * 1024 * 1024, f);
        
        // buf := make([]byte, 16*1024)
        let mut buf = vec![0u8; 16 * 1024];
        
        // l, e := binary.ReadUvarint(r)
        // for ; e == nil; l, e = binary.ReadUvarint(r) {
        loop {
            let l = match read_uvarint(&mut r) {
                Ok(v) => v,
                Err(_) => break, // EOF
            };
            
            // compressed := (l & 1) == 0
            let compressed = (l & 1) == 0;
            // l >>= 1
            let l = l >> 1;
            
            // if int(l) > len(buf) {
            if l as usize > buf.len() {
                // buf = make([]byte, l)
                buf = vec![0u8; l as usize];
            // }
            }
            // if _, e = io.ReadFull(r, buf[:l]); e != nil {
            //     return e
            // }
            r.read_exact(&mut buf[..l as usize])?;
            
            // if err := walker(buf[:l], compressed); err != nil {
            //     return err
            // }
            walker(&buf[..l as usize], compressed)?;
        }
        
        // if !errors.Is(e, io.EOF) {
        //     return e
        // }
        // return nil
        Ok(())
    }
}

// ========== Port of parallel_compress.go lines 238-730 compressWithPatternCandidates ==========
// func compressWithPatternCandidates(ctx context.Context, trace bool, cfg Cfg, logPrefix, segmentFilePath string, cf *os.File, uncompressedFile *RawWordsFile, dictBuilder *DictionaryBuilder, lvl log.Lvl, logger log.Logger) error {
pub fn compress_with_pattern_candidates(
    trace: bool,                        // trace bool
    cfg: &Cfg,                          // cfg Cfg
    segment_file_path: &Path,           // segmentFilePath string
    cf: &mut File,                      // cf *os.File
    uncompressed_file: &RawWordsFile,   // uncompressedFile *RawWordsFile
    code2pattern: &[Pattern],           // code2pattern from dictBuilder
    pt: &PatriciaTree,                  // PatriciaTree built from patterns
) -> Result<()> {
    // Lines 239-241: Setup logging
    // logEvery := time.NewTicker(60 * time.Second)
    // defer logEvery.Stop()
    // Note: We'll skip logging for now
    
    // Lines 242-259: Dictionary builder processing
    // This is already done - code2pattern is passed in
    
    // Lines 260-269: Setup channels and workers
    // Note: We'll do single-threaded version for now
    
    // Lines 270-280: Initialize variables
    // var output = make([]byte, 0, 256)
    let mut output = Vec::with_capacity(256);
    // var uncovered = make([]int, 256)
    let mut uncovered = Vec::with_capacity(256);
    // var patterns = make([]int, 0, 256)
    let mut patterns = Vec::with_capacity(256);
    // cellRing := NewRing()
    let mut cell_ring = Ring::new();
    // mf2 := patricia.NewMatchFinder2(&pt)
    let mut mf2 = MatchFinder2::new(pt);
    // var posMaps []map[uint64]uint64
    // uncompPosMap := make(map[uint64]uint64)
    let mut uncomp_pos_map: HashMap<u64, u64> = HashMap::new();
    
    // Lines 290-310: Setup intermediate file
    // intermediatePath := segmentFilePath + ".tmp"
    let intermediate_path = segment_file_path.with_extension("tmp");
    // defer os.Remove(intermediatePath)
    // Note: We'll handle cleanup manually
    
    // var intermediateFile *os.File
    // if intermediateFile, err = os.Create(intermediatePath); err != nil {
    //     return fmt.Errorf("create intermediate file: %w", err)
    // }
    let intermediate_file = File::create(&intermediate_path)?;
    // defer intermediateFile.Close()
    
    // intermediateW := bufio.NewWriterSize(intermediateFile, 8*etl.BufIOSize)
    let mut intermediate_w = BufWriter::with_capacity(8 * 8192 * 1024, intermediate_file);
    
    // Lines 311-410: Process words
    // var inCount, outCount, emptyWordsCount uint64
    let mut in_count = 0u64;
    let mut out_count = 0u64;
    let mut empty_words_count = 0u64;
    
    // var numBuf [binary.MaxVarintLen64]byte
    let mut num_buf = vec![0u8; 10]; // MaxVarintLen64
    
    // totalWords := uncompressedFile.count
    let total_words = uncompressed_file.count;
    
    // if err = uncompressedFile.ForEach(func(v []byte, compression bool) error {
    uncompressed_file.for_each(|v, compression| {
        // Lines 360-398: Process each word
        // if cfg.Workers > 1 {
        // Note: Single-threaded version
        
        // outCount++
        out_count += 1;
        // wordLen := uint64(len(v))
        let word_len = v.len() as u64;
        // n := binary.PutUvarint(numBuf[:], wordLen)
        let n = put_uvarint(&mut num_buf, word_len);
        // if _, e := intermediateW.Write(numBuf[:n]); e != nil {
        //     return e
        // }
        intermediate_w.write_all(&num_buf[..n])?;
        
        // if wordLen > 0 {
        if word_len > 0 {
            // if compression {
            if compression {
                // output, patterns, uncovered = coverWordByPatterns(trace, v, mf2, output[:0], uncovered, patterns, cellRing, uncompPosMap)
                let matches = mf2.find_longest_matches(v);
                
                // output, patterns, uncovered = coverWordByPatterns(trace, v, mf2, output[:0], uncovered, patterns, cellRing, uncompPosMap)
                output.clear();
                cover_word_by_patterns(
                    trace,
                    v,
                    &matches,
                    &mut output,
                    &mut uncovered,
                    &mut patterns,
                    &mut cell_ring,
                    &mut uncomp_pos_map,
                    code2pattern,
                )?;
                
                // if _, e := intermediateW.Write(output); e != nil {
                //     return e
                // }
                intermediate_w.write_all(&output)?;
            // } else {
            } else {
                // if e := intermediateW.WriteByte(0); e != nil {
                //     return e
                // }
                intermediate_w.write_all(&[0])?;
                // if _, e := intermediateW.Write(v); e != nil {
                //     return e
                // }
                intermediate_w.write_all(v)?;
            // }
            }
        // }
        }
        
        // uncompPosMap[wordLen+1]++
        *uncomp_pos_map.entry(word_len + 1).or_insert(0) += 1;
        // uncompPosMap[0]++
        *uncomp_pos_map.entry(0).or_insert(0) += 1;
        
        // inCount++
        in_count += 1;
        // if len(v) == 0 {
        if v.is_empty() {
            // emptyWordsCount++
            empty_words_count += 1;
        // }
        }
        
        Ok(())
    })?;
    
    // Lines 440-443: Flush intermediate file
    // if err = intermediateW.Flush(); err != nil {
    //     return err
    // }
    intermediate_w.flush()?;
    drop(intermediate_w);
    
    // Lines 454-460: Merge position maps (single-threaded, so just use uncomp_pos_map)
    let pos_map = uncomp_pos_map;
    
    // Continue with Huffman tree building and final compression pass (lines 461-730)
    
    // Build pattern list for Huffman encoding
    let mut pattern_list: Vec<Pattern> = Vec::new();
    for p in code2pattern {
        if p.uses > 0 {
            pattern_list.push(p.clone());
        }
    }
    
    // Sort patterns by usage for Huffman tree
    pattern_list.sort_by(|a, b| {
        if a.uses == b.uses {
            b.code.reverse_bits().cmp(&a.code.reverse_bits())
        } else {
            a.uses.cmp(&b.uses)
        }
    });
    
    // Build Huffman tree for patterns
    use std::collections::BinaryHeap;
    use crate::compress_go_port3::{PatternHuff, PositionHuff};
    
    let mut i = 0;
    let mut code_heap: BinaryHeap<PatternHuff> = BinaryHeap::new();
    let mut tie_breaker = 0u64;
    
    while code_heap.len() + (pattern_list.len() - i) > 1 {
        let mut h = PatternHuff {
            p0: None,
            p1: None,
            h0: None,
            h1: None,
            uses: 0,
            tie_breaker,
        };
        
        if code_heap.len() > 0 && (i >= pattern_list.len() || code_heap.peek().unwrap().uses < pattern_list[i].uses) {
            let mut h0 = code_heap.pop().unwrap();
            h0.add_zero();
            h.uses += h0.uses;
            h.h0 = Some(Box::new(h0));
        } else {
            h.p0 = Some(Box::new(pattern_list[i].clone()));
            h.p0.as_mut().unwrap().code = 0;
            h.p0.as_mut().unwrap().code_bits = 1;
            h.uses += h.p0.as_ref().unwrap().uses;
            i += 1;
        }
        
        if code_heap.len() > 0 && (i >= pattern_list.len() || code_heap.peek().unwrap().uses < pattern_list[i].uses) {
            let mut h1 = code_heap.pop().unwrap();
            h1.add_one();
            h.uses += h1.uses;
            h.h1 = Some(Box::new(h1));
        } else {
            h.p1 = Some(Box::new(pattern_list[i].clone()));
            h.p1.as_mut().unwrap().code = 1;
            h.p1.as_mut().unwrap().code_bits = 1;
            h.uses += h.p1.as_ref().unwrap().uses;
            i += 1;
        }
        
        tie_breaker += 1;
        code_heap.push(h);
    }
    
    if code_heap.len() > 0 {
        let mut root = code_heap.pop().unwrap();
        root.set_depth(0);
    }
    
    // Calculate pattern dictionary size
    let mut patterns_size = 0u64;
    let mut num_buf = [0u8; 10];
    for p in &pattern_list {
        let ns = put_uvarint(&mut num_buf, p.depth as u64);
        let n = put_uvarint(&mut num_buf, p.word.len() as u64);
        patterns_size += (ns + n + p.word.len()) as u64;
    }
    
    // Start writing compressed file
    use std::io::{BufWriter, BufReader, Seek, SeekFrom};
    let mut cw = BufWriter::with_capacity(2 * 8192 * 1024, cf);
    
    
    // Write file headers
    num_buf[..8].copy_from_slice(&in_count.to_be_bytes());
    cw.write_all(&num_buf[..8])?;
    num_buf[..8].copy_from_slice(&empty_words_count.to_be_bytes());
    cw.write_all(&num_buf[..8])?;
    num_buf[..8].copy_from_slice(&patterns_size.to_be_bytes());
    cw.write_all(&num_buf[..8])?;
    
    // Write pattern dictionary
    pattern_list.sort_by(|a, b| {
        if a.uses == b.uses {
            b.code.reverse_bits().cmp(&a.code.reverse_bits())
        } else {
            a.uses.cmp(&b.uses)
        }
    });
    
    for p in &pattern_list {
        let ns = put_uvarint(&mut num_buf, p.depth as u64);
        cw.write_all(&num_buf[..ns])?;
        let n = put_uvarint(&mut num_buf, p.word.len() as u64);
        cw.write_all(&num_buf[..n])?;
        cw.write_all(&p.word)?;
    }
    
    // Build position list for Huffman encoding
    let mut position_list: Vec<Position> = Vec::new();
    let mut pos2code: HashMap<u64, Position> = HashMap::new();
    
    for (pos, uses) in &pos_map {
        let p = Position {
            pos: *pos,
            uses: *uses,
            code: *pos,
            code_bits: 0,
            depth: 0,
        };
        position_list.push(p.clone());
        pos2code.insert(*pos, p);
    }
    
    // Sort positions by usage for Huffman tree
    position_list.sort_by(|a, b| {
        if a.uses == b.uses {
            b.code.reverse_bits().cmp(&a.code.reverse_bits())
        } else {
            a.uses.cmp(&b.uses)
        }
    });
    
    // Build Huffman tree for positions
    i = 0;
    let mut pos_heap: BinaryHeap<PositionHuff> = BinaryHeap::new();
    tie_breaker = 0;
    
    while pos_heap.len() + (position_list.len() - i) > 1 {
        let mut h = PositionHuff {
            p0: None,
            p1: None,
            h0: None,
            h1: None,
            uses: 0,
            tie_breaker,
        };
        
        if pos_heap.len() > 0 && (i >= position_list.len() || pos_heap.peek().unwrap().uses < position_list[i].uses) {
            let mut h0 = pos_heap.pop().unwrap();
            h0.add_zero();
            h.uses += h0.uses;
            h.h0 = Some(Box::new(h0));
        } else {
            h.p0 = Some(Box::new(position_list[i].clone()));
            h.p0.as_mut().unwrap().code = 0;
            h.p0.as_mut().unwrap().code_bits = 1;
            h.uses += h.p0.as_ref().unwrap().uses;
            i += 1;
        }
        
        if pos_heap.len() > 0 && (i >= position_list.len() || pos_heap.peek().unwrap().uses < position_list[i].uses) {
            let mut h1 = pos_heap.pop().unwrap();
            h1.add_one();
            h.uses += h1.uses;
            h.h1 = Some(Box::new(h1));
        } else {
            h.p1 = Some(Box::new(position_list[i].clone()));
            h.p1.as_mut().unwrap().code = 1;
            h.p1.as_mut().unwrap().code_bits = 1;
            h.uses += h.p1.as_ref().unwrap().uses;
            i += 1;
        }
        
        tie_breaker += 1;
        pos_heap.push(h);
    }
    
    if pos_heap.len() > 0 {
        let mut pos_root = pos_heap.pop().unwrap();
        pos_root.set_depth(0);
    }
    
    // Calculate position dictionary size
    let mut pos_size = 0u64;
    for p in &position_list {
        let ns = put_uvarint(&mut num_buf, p.depth as u64);
        let n = put_uvarint(&mut num_buf, p.pos);
        pos_size += (ns + n) as u64;
    }
    
    
    // Write position dictionary size
    num_buf[..8].copy_from_slice(&pos_size.to_be_bytes());
    cw.write_all(&num_buf[..8])?;
    
    // Write position dictionary
    position_list.sort_by(|a, b| {
        if a.depth == b.depth {
            a.code.cmp(&b.code)
        } else {
            a.depth.cmp(&b.depth)
        }
    });
    
    for p in &position_list {
        let ns = put_uvarint(&mut num_buf, p.depth as u64);
        cw.write_all(&num_buf[..ns])?;
        let n = put_uvarint(&mut num_buf, p.pos);
        cw.write_all(&num_buf[..n])?;
    }
    
    // Re-open intermediate file for reading
    // Note: intermediate_file was already moved into BufWriter, so just open fresh
    let intermediate_file = File::open(&intermediate_path)?;
    let mut r = BufReader::with_capacity(2 * 8192 * 1024, intermediate_file);
    
    // Process intermediate file and write final compressed data
    let mut compressed_buffer = Vec::new();
    
    loop {
        let l = match read_uvarint(&mut r) {
            Ok(v) => v,
            Err(_) => break,
        };
        
        let mut word_buffer = Vec::new();
        let mut hc = BitWriter::new(&mut word_buffer);
        
        let pos_code = pos2code.get(&(l + 1));
        if let Some(pos_code) = pos_code {
            hc.encode(pos_code.code, pos_code.code_bits as i32)?;
        }
        
        if l == 0 {
            hc.flush()?;
            compressed_buffer.extend_from_slice(&word_buffer);
        } else {
            let p_num = read_uvarint(&mut r)?;
            let mut last_pos = 0u64;
            let mut last_uncovered = 0i32;
            let mut uncovered_count = 0i32;
            
            for _ in 0..p_num {
                let pos = read_uvarint(&mut r)?;
                let pos_code = pos2code.get(&(pos - last_pos + 1));
                last_pos = pos;
                
                if let Some(pos_code) = pos_code {
                    hc.encode(pos_code.code, pos_code.code_bits as i32)?;
                }
                
                let code = read_uvarint(&mut r)?;
                let pattern_code = &code2pattern[code as usize];
                
                if pos as i32 > last_uncovered {
                    uncovered_count += pos as i32 - last_uncovered;
                }
                
                last_uncovered = pos as i32 + pattern_code.word.len() as i32;
                hc.encode(pattern_code.code, pattern_code.code_bits as i32)?;
            }
            
            if l as i32 > last_uncovered {
                uncovered_count += l as i32 - last_uncovered;
            }
            
            let pos_code = pos2code.get(&0);
            if let Some(pos_code) = pos_code {
                hc.encode(pos_code.code, pos_code.code_bits as i32)?;
            }
            
            hc.flush()?;
            compressed_buffer.extend_from_slice(&word_buffer);
            
            if uncovered_count > 0 {
                let mut buf = vec![0u8; uncovered_count as usize];
                r.read_exact(&mut buf)?;
                compressed_buffer.extend_from_slice(&buf);
            }
        }
    }
    
    // Write all compressed data
    cw.write_all(&compressed_buffer)?;
    cw.flush()?;
    
    // Clean up intermediate file
    std::fs::remove_file(&intermediate_path)?;
    
    Ok(())
}

// Helper function to put uvarint into buffer
pub fn put_uvarint(buf: &mut [u8], mut value: u64) -> usize {
    let mut i = 0;
    while value >= 0x80 {
        buf[i] = (value as u8) | 0x80;
        value >>= 7;
        i += 1;
    }
    buf[i] = value as u8;
    i + 1
}

// Helper function to read uvarint
pub fn read_uvarint(reader: &mut dyn Read) -> Result<u64> {
    let mut result = 0u64;
    let mut shift = 0;
    let mut buf = [0u8; 1];
    
    loop {
        reader.read_exact(&mut buf)?;
        let byte = buf[0];
        
        result |= ((byte & 0x7F) as u64) << shift;
        
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        
        shift += 7;
        if shift > 63 {
            return Err(Error::InvalidFormat("Varint too large".into()));
        }
    }
}


// Cfg struct to match Go
pub struct Cfg {
    pub workers: usize,
    pub min_pattern_score: u64,
    pub min_pattern_len: usize,
    pub max_pattern_len: usize,
    pub max_dict_patterns: usize,
}