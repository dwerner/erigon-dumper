// Exact line-by-line port of Go's compress.go and parallel_compress.go
// Each Go line is included as a comment above its Rust equivalent

use std::collections::HashMap;
use std::io::{Write, Read, BufWriter, BufReader};
use std::fs::File;
use std::path::Path;
use crate::error::{Result, Error};
use crate::patricia::Match;
use log::debug;

// Helper to print byte slices as hex
fn print_hex(bytes: &[u8]) {
    for byte in bytes {
        print!("{:02x}", byte);
    }
}

// ========== Port of compress.go lines 677-685 ==========
// type DynamicCell struct {
//     optimStart  int
//     coverStart  int
//     compression int
//     score       uint64
//     patternIdx  int // offset of the last element in the pattern slice
// }
#[derive(Clone, Copy, Debug)]
pub struct DynamicCell {
    pub optim_start: i32,      // int
    pub cover_start: i32,      // int  
    pub compression: i32,      // int
    pub score: u64,            // uint64
    pub pattern_idx: i32,      // int
}

// ========== Port of compress.go lines 686-689 ==========
// type Ring struct {
//     cells             []DynamicCell
//     head, tail, count int
// }
pub struct Ring {
    pub cells: Vec<DynamicCell>,
    pub head: i32,     // int
    pub tail: i32,     // int
    pub count: i32,    // int
}

// ========== Port of compress.go lines 691-698 ==========
// func NewRing() *Ring {
//     return &Ring{
//         cells: make([]DynamicCell, 16),
//         head:  0,
//         tail:  0,
//         count: 0,
//     }
// }
impl Ring {
    pub fn new() -> Self {
        // cells: make([]DynamicCell, 16),
        let mut cells = Vec::with_capacity(16);
        for _ in 0..16 {
            cells.push(DynamicCell {
                optim_start: 0,
                cover_start: 0,
                compression: 0,
                score: 0,
                pattern_idx: 0,
            });
        }
        // return &Ring{
        Ring {
            // cells: make([]DynamicCell, 16),
            cells,
            // head:  0,
            head: 0,
            // tail:  0,
            tail: 0,
            // count: 0,
            count: 0,
        }
    }

    // ========== Port of compress.go lines 700-704 ==========
    // func (r *Ring) Reset() {
    //     r.count = 0
    //     r.head = 0
    //     r.tail = 0
    // }
    pub fn reset(&mut self) {
        // r.count = 0
        self.count = 0;
        // r.head = 0
        self.head = 0;
        // r.tail = 0
        self.tail = 0;
    }

    // ========== Port of compress.go lines 706-720 ==========
    // func (r *Ring) ensureSize() {
    //     if r.count < len(r.cells) {
    //         return
    //     }
    //     newcells := make([]DynamicCell, r.count*2)
    //     if r.tail > r.head {
    //         copy(newcells, r.cells[r.head:r.tail])
    //     } else {
    //         n := copy(newcells, r.cells[r.head:])
    //         copy(newcells[n:], r.cells[:r.tail])
    //     }
    //     r.head = 0
    //     r.tail = r.count
    //     r.cells = newcells
    // }
    fn ensure_size(&mut self) {
        // if r.count < len(r.cells) {
        if self.count < self.cells.len() as i32 {
            // return
            return;
        }
        // newcells := make([]DynamicCell, r.count*2)
        let mut newcells = Vec::with_capacity((self.count * 2) as usize);
        for _ in 0..(self.count * 2) {
            newcells.push(DynamicCell {
                optim_start: 0,
                cover_start: 0,
                compression: 0,
                score: 0,
                pattern_idx: 0,
            });
        }
        // if r.tail > r.head {
        if self.tail > self.head {
            // copy(newcells, r.cells[r.head:r.tail])
            for i in self.head..self.tail {
                newcells[(i - self.head) as usize] = self.cells[i as usize].clone();
            }
        } else {
            // n := copy(newcells, r.cells[r.head:])
            let mut n = 0;
            for i in self.head..(self.cells.len() as i32) {
                newcells[n] = self.cells[i as usize].clone();
                n += 1;
            }
            // copy(newcells[n:], r.cells[:r.tail])
            for i in 0..self.tail {
                newcells[n] = self.cells[i as usize].clone();
                n += 1;
            }
        }
        // r.head = 0
        self.head = 0;
        // r.tail = r.count
        self.tail = self.count;
        // r.cells = newcells
        self.cells = newcells;
    }

    // ========== Port of compress.go lines 722-730 ==========
    // func (r *Ring) PushFront() *DynamicCell {
    //     r.ensureSize()
    //     if r.head == 0 {
    //         r.head = len(r.cells)
    //     }
    //     r.head--
    //     r.count++
    //     return &r.cells[r.head]
    // }
    pub fn push_front(&mut self) -> &mut DynamicCell {
        // r.ensureSize()
        self.ensure_size();
        // if r.head == 0 {
        if self.head == 0 {
            // r.head = len(r.cells)
            self.head = self.cells.len() as i32;
        }
        // r.head--
        self.head -= 1;
        // r.count++
        self.count += 1;
        // return &r.cells[r.head]
        &mut self.cells[self.head as usize]
    }

    // ========== Port of compress.go lines 732-741 ==========
    // func (r *Ring) PushBack() *DynamicCell {
    //     r.ensureSize()
    //     if r.tail == len(r.cells) {
    //         r.tail = 0
    //     }
    //     result := &r.cells[r.tail]
    //     r.tail++
    //     r.count++
    //     return result
    // }
    pub fn push_back(&mut self) -> &mut DynamicCell {
        // r.ensureSize()
        self.ensure_size();
        // if r.tail == len(r.cells) {
        if self.tail == self.cells.len() as i32 {
            // r.tail = 0
            self.tail = 0;
        }
        // result := &r.cells[r.tail]
        let result_idx = self.tail as usize;
        // r.tail++
        self.tail += 1;
        // r.count++
        self.count += 1;
        // return result
        &mut self.cells[result_idx]
    }

    // ========== Port of compress.go lines 743-745 ==========
    // func (r Ring) Len() int {
    //     return r.count
    // }
    pub fn len(&self) -> i32 {
        // return r.count
        self.count
    }

    // ========== Port of compress.go lines 747-752 ==========
    // func (r *Ring) Get(i int) *DynamicCell {
    //     if i < 0 || i >= r.count {
    //         return nil
    //     }
    //     return &r.cells[(r.head+i)&(len(r.cells)-1)]
    // }
    pub fn get(&self, i: i32) -> Option<&DynamicCell> {
        // if i < 0 || i >= r.count {
        if i < 0 || i >= self.count {
            // return nil
            return None;
        }
        // return &r.cells[(r.head+i)&(len(r.cells)-1)]
        Some(&self.cells[((self.head + i) & (self.cells.len() as i32 - 1)) as usize])
    }

    // ========== Port of compress.go lines 754-758 ==========
    // func (r *Ring) Truncate(i int) {
    //     r.count = i
    //     r.tail = (r.head + i) & (len(r.cells) - 1)
    // }
    pub fn truncate(&mut self, i: i32) {
        // r.count = i
        self.count = i;
        // r.tail = (r.head + i) & (len(r.cells) - 1)
        self.tail = (self.head + i) & (self.cells.len() as i32 - 1);
    }
}

// ========== Port of compress.go lines 462-468 ==========
// type Pattern struct {
//     word     []byte // Pattern bytes
//     score    uint64 // Score assigned to the pattern during building the dictionary
//     uses     uint64 // Number of uses of this pattern during compression
//     code     uint64 // Allocated numerical code for this pattern
//     codeBits int    // Number of bits in the code
//     depth    int    // Depth of the pattern in the huffman tree (for encoding in the file)
// }
#[derive(Clone, Debug)]
pub struct Pattern {
    pub word: Vec<u8>,      // []byte
    pub score: u64,         // uint64
    pub uses: u64,          // uint64
    pub code: u64,          // uint64
    pub code_bits: i32,     // int
    pub depth: i32,         // int
}


// ========== Port of parallel_compress.go lines 7-179 coverWordByPatterns ==========
// func coverWordByPatterns(trace bool, input []byte, mf2 *patricia.MatchFinder2, output []byte, uncovered []int, patterns []int, cellRing *Ring, posMap map[uint64]uint64) ([]byte, []int, []int) {
pub fn cover_word_by_patterns(
    trace: bool,                           // trace bool
    input: &[u8],                          // input []byte
    matches: &[Match],                     // matches from mf2.FindLongestMatches(input)
    output: &mut Vec<u8>,                  // output []byte
    uncovered: &mut Vec<i32>,              // uncovered []int
    patterns: &mut Vec<i32>,               // patterns []int
    cell_ring: &mut Ring,                  // cellRing *Ring
    pos_map: &mut HashMap<u64, u64>,       // posMap map[uint64]uint64
    code2pattern: &mut [Pattern],          // code2pattern []*Pattern - pattern array (mutable)
) -> Result<()> {
    // matches := mf2.FindLongestMatches(input)
    // Note: matches is passed in as parameter since we don't have patricia tree yet

    // ========== Port of parallel_compress.go lines 10-14 ==========
    // if len(matches) == 0 {
    //     output = append(output, 0) // Encoding of 0 in VarUint is 1 zero byte
    //     output = append(output, input...)
    //     return output, patterns, uncovered
    // }
    if matches.len() == 0 {
        // output = append(output, 0)
        output.push(0);
        // output = append(output, input...)
        output.extend_from_slice(input);
        // return output, patterns, uncovered
        return Ok(());
    }

    // ========== Port of parallel_compress.go lines 15-21 ==========
    // if trace {
    //     fmt.Printf("Cluster | input = %x\n", input)
    //     for _, match := range matches {
    //         fmt.Printf(" [%x %d-%d]", input[match.Start:match.End], match.Start, match.End)
    //     }
    // }
    if trace {
        // fmt.Printf("Cluster | input = %x\n", input)
        print!("Cluster | input = ");
        print_hex(input);
        println!();
        // for _, match := range matches {
        for m in matches {
            // fmt.Printf(" [%x %d-%d]", input[match.Start:match.End], match.Start, match.End)
            print!(" [");
            print_hex(&input[m.start as usize..m.end as usize]);
            print!(" {}-{}]", m.start, m.end);
        }
    }

    // ========== Port of parallel_compress.go lines 22-23 ==========
    // cellRing.Reset()
    // patterns = append(patterns[:0], 0, 0) // Sentinel entry - no meaning
    cell_ring.reset();
    // patterns = append(patterns[:0], 0, 0)
    patterns.clear();
    patterns.push(0);
    patterns.push(0);

    // ========== Port of parallel_compress.go lines 24-34 ==========
    // lastF := matches[len(matches)-1]
    // for j := lastF.Start; j < lastF.End; j++ {
    //     d := cellRing.PushBack()
    //     d.optimStart = j + 1
    //     d.coverStart = len(input)
    //     d.compression = 0
    //     d.patternIdx = 0
    //     d.score = 0
    // }
    let last_f = &matches[matches.len() - 1];
    // for j := lastF.Start; j < lastF.End; j++ {
    for j in last_f.start..last_f.end {
        // d := cellRing.PushBack()
        let d = cell_ring.push_back();
        // d.optimStart = j + 1
        d.optim_start = j + 1;
        // d.coverStart = len(input)
        d.cover_start = input.len() as i32;
        // d.compression = 0
        d.compression = 0;
        // d.patternIdx = 0
        d.pattern_idx = 0;
        // d.score = 0
        d.score = 0;
    }

    // ========== Port of parallel_compress.go lines 35-128 ==========
    // Starting from the last match
    // for i := len(matches); i > 0; i-- {
    for i in (1..=matches.len()).rev() {
        // f := matches[i-1]
        let f = &matches[i - 1];
        // p := f.Val.(*Pattern)
        let p = &code2pattern[f.val];
        
        // firstCell := cellRing.Get(0)
        let first_cell = cell_ring.get(0).unwrap().clone();
        // maxCompression := firstCell.compression
        let mut max_compression = first_cell.compression;
        // maxScore := firstCell.score
        let mut max_score = first_cell.score;
        // maxCell := firstCell
        let mut max_cell = first_cell.clone();
        // var maxInclude bool
        let mut max_include = false;
        
        // for e := 0; e < cellRing.Len(); e++ {
        let mut e = 0;
        while e < cell_ring.len() {
            // cell := cellRing.Get(e)
            let cell = cell_ring.get(e).unwrap().clone();
            // comp := cell.compression - 4
            let mut comp = cell.compression - 4;
            // if cell.coverStart >= f.End {
            if cell.cover_start >= f.end {
                // comp += f.End - f.Start
                comp += f.end - f.start;
            } else {
                // comp += cell.coverStart - f.Start
                comp += cell.cover_start - f.start;
            }
            // score := cell.score + p.score
            let score = cell.score + p.score;
            // if comp > maxCompression || (comp == maxCompression && score > maxScore) {
            if comp > max_compression || (comp == max_compression && score > max_score) {
                // maxCompression = comp
                max_compression = comp;
                // maxScore = score
                max_score = score;
                // maxInclude = true
                max_include = true;
                // maxCell = cell
                max_cell = cell;
            // } else if cell.optimStart > f.End {
            } else if cell.optim_start > f.end {
                // cellRing.Truncate(e)
                cell_ring.truncate(e);
                // break
                break;
            }
            e += 1;
        }
        
        // d := cellRing.PushFront()
        let d = cell_ring.push_front();
        // d.optimStart = f.Start
        d.optim_start = f.start;
        // d.score = maxScore
        d.score = max_score;
        // d.compression = maxCompression
        d.compression = max_compression;
        
        // if maxInclude {
        if max_include {
            // if trace {
            if trace {
                // fmt.Printf("[include] cell for %d: with patterns", f.Start)
                print!("[include] cell for {}: with patterns", f.start);
                // fmt.Printf(" [%x %d-%d]", input[f.Start:f.End], f.Start, f.End)
                print!(" [");
                print_hex(&input[f.start as usize..f.end as usize]);
                print!(" {}-{}]", f.start, f.end);
                // patternIdx := maxCell.patternIdx
                let mut pattern_idx = max_cell.pattern_idx;
                // for patternIdx != 0 {
                while pattern_idx != 0 {
                    // pattern := patterns[patternIdx]
                    let pattern = patterns[pattern_idx as usize];
                    // fmt.Printf(" [%x %d-%d]", input[matches[pattern].Start:matches[pattern].End], matches[pattern].Start, matches[pattern].End)
                    let m = &matches[pattern as usize];
                    print!(" [");
            print_hex(&input[m.start as usize..m.end as usize]);
            print!(" {}-{}]", m.start, m.end);
                    // patternIdx = patterns[patternIdx+1]
                    pattern_idx = patterns[(pattern_idx + 1) as usize];
                }
                // fmt.Printf("\n\n")
                println!("\n");
            }
            // d.coverStart = f.Start
            d.cover_start = f.start;
            // d.patternIdx = len(patterns)
            d.pattern_idx = patterns.len() as i32;
            // patterns = append(patterns, i-1, maxCell.patternIdx)
            patterns.push((i - 1) as i32);
            patterns.push(max_cell.pattern_idx);
            debug!("  Selected pattern at match index {}", i - 1);
        } else {
            // if trace {
            if trace {
                // fmt.Printf("cell for %d: with patterns", f.Start)
                print!("cell for {}: with patterns", f.start);
                // patternIdx := maxCell.patternIdx
                let mut pattern_idx = max_cell.pattern_idx;
                // for patternIdx != 0 {
                while pattern_idx != 0 {
                    // pattern := patterns[patternIdx]
                    let pattern = patterns[pattern_idx as usize];
                    // fmt.Printf(" [%x %d-%d]", input[matches[pattern].Start:matches[pattern].End], matches[pattern].Start, matches[pattern].End)
                    let m = &matches[pattern as usize];
                    print!(" [");
            print_hex(&input[m.start as usize..m.end as usize]);
            print!(" {}-{}]", m.start, m.end);
                    // patternIdx = patterns[patternIdx+1]
                    pattern_idx = patterns[(pattern_idx + 1) as usize];
                }
                // fmt.Printf("\n\n")
                println!("\n");
            }
            // d.coverStart = maxCell.coverStart
            d.cover_start = max_cell.cover_start;
            // d.patternIdx = maxCell.patternIdx
            d.pattern_idx = max_cell.pattern_idx;
        }
    }

    // ========== Port of parallel_compress.go lines 129-142 ==========
    // optimCell := cellRing.Get(0)
    let optim_cell = cell_ring.get(0).unwrap().clone();
    // if trace {
    if trace {
        // fmt.Printf("optimal =")
        print!("optimal =");
    }
    // Count number of patterns
    // var patternCount uint64
    let mut pattern_count = 0u64;
    // patternIdx := optimCell.patternIdx
    let mut pattern_idx = optim_cell.pattern_idx;
    // for patternIdx != 0 {
    while pattern_idx != 0 {
        // patternCount++
        pattern_count += 1;
        // patternIdx = patterns[patternIdx+1]
        pattern_idx = patterns[(pattern_idx + 1) as usize];
    }
    // var numBuf [binary.MaxVarintLen64]byte
    // p := binary.PutUvarint(numBuf[:], patternCount)
    // output = append(output, numBuf[:p]...)
    write_varint(output, pattern_count)?;

    // ========== Port of parallel_compress.go lines 143-169 ==========
    // patternIdx = optimCell.patternIdx
    pattern_idx = optim_cell.pattern_idx;
    // lastStart := 0
    let mut last_start = 0i32;
    // var lastUncovered int
    let mut last_uncovered = 0i32;
    // uncovered = uncovered[:0]
    uncovered.clear();
    
    // for patternIdx != 0 {
    while pattern_idx != 0 {
        // pattern := patterns[patternIdx]
        let pattern = patterns[pattern_idx as usize];
        // p := matches[pattern].Val.(*Pattern)
        let m = &matches[pattern as usize];
        let p = &code2pattern[m.val];
        
        // if trace {
        if trace {
            // fmt.Printf(" [%x %d-%d]", input[matches[pattern].Start:matches[pattern].End], matches[pattern].Start, matches[pattern].End)
            print!(" [");
            print_hex(&input[m.start as usize..m.end as usize]);
            print!(" {}-{}]", m.start, m.end);
        }
        
        // if matches[pattern].Start > lastUncovered {
        if m.start > last_uncovered {
            // uncovered = append(uncovered, lastUncovered, matches[pattern].Start)
            uncovered.push(last_uncovered);
            uncovered.push(m.start);
        }
        // lastUncovered = matches[pattern].End
        last_uncovered = m.end;
        
        // Starting position
        // posMap[uint64(matches[pattern].Start-lastStart+1)]++
        *pos_map.entry((m.start - last_start + 1) as u64).or_insert(0) += 1;
        // lastStart = matches[pattern].Start
        last_start = m.start;
        
        // n := binary.PutUvarint(numBuf[:], uint64(matches[pattern].Start))
        // output = append(output, numBuf[:n]...)
        write_varint(output, m.start as u64)?;
        
        // Code
        // n = binary.PutUvarint(numBuf[:], p.code)
        // output = append(output, numBuf[:n]...)
        write_varint(output, p.code)?;
        
        // atomic.AddUint64(&p.uses, 1)
        // Directly increment the pattern's uses field (like Go)
        if let Some(pattern) = code2pattern.get_mut(p.code as usize) {
            pattern.uses += 1;
        }
        
        // patternIdx = patterns[patternIdx+1]
        pattern_idx = patterns[(pattern_idx + 1) as usize];
    }
    
    // if len(input) > lastUncovered {
    if (input.len() as i32) > last_uncovered {
        // uncovered = append(uncovered, lastUncovered, len(input))
        uncovered.push(last_uncovered);
        uncovered.push(input.len() as i32);
    }

    // ========== Port of parallel_compress.go lines 170-178 ==========
    // if trace {
    if trace {
        // fmt.Printf("\n\n")
        println!("\n");
    }
    // Add uncoded input
    // for i := 0; i < len(uncovered); i += 2 {
    for i in (0..uncovered.len()).step_by(2) {
        // output = append(output, input[uncovered[i]:uncovered[i+1]]...)
        if i + 1 < uncovered.len() {
            output.extend_from_slice(&input[uncovered[i] as usize..uncovered[i + 1] as usize]);
        }
    }
    // return output, patterns, uncovered
    Ok(())
}

// Helper function to write varint
fn write_varint(output: &mut Vec<u8>, mut value: u64) -> Result<()> {
    loop {
        if value < 0x80 {
            output.push(value as u8);
            return Ok(());
        }
        output.push(((value & 0x7F) | 0x80) as u8);
        value >>= 7;
    }
}