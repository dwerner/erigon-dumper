// Continuation of exact line-by-line port - Huffman tree building and final compression
// Port of parallel_compress.go lines 461-730

use std::collections::{HashMap, BinaryHeap};
use std::io::{Write, Read, BufWriter, BufReader, Seek, SeekFrom};
use std::fs::File;
use std::cmp::Ordering;
use crate::error::{Result, Error};
use crate::compress_go_port::{Pattern};
use crate::compress_go_port2::{Position, BitWriter, put_uvarint, read_uvarint};

// ========== Port of compress.go lines 470-475 PatternHuff ==========
// type PatternHuff struct {
//     p0, p1       *Pattern
//     h0, h1       *PatternHuff
//     uses         uint64
//     tieBreaker uint64
// }
#[derive(Clone)]
pub struct PatternHuff {
    pub p0: Option<Box<Pattern>>,      // *Pattern
    pub p1: Option<Box<Pattern>>,      // *Pattern
    pub h0: Option<Box<PatternHuff>>,  // *PatternHuff
    pub h1: Option<Box<PatternHuff>>,  // *PatternHuff
    pub uses: u64,                     // uint64
    pub tie_breaker: u64,              // uint64
}

impl PatternHuff {
    // func (h *PatternHuff) AddZero() {
    pub fn add_zero(&mut self) {
        // if h.p0 != nil {
        if let Some(ref mut p0) = self.p0 {
            // h.p0.code <<= 1
            p0.code <<= 1;
            // h.p0.codeBits++
            p0.code_bits += 1;
        // } else {
        } else if let Some(ref mut h0) = self.h0 {
            // h.h0.AddZero()
            h0.add_zero();
        // }
        }
        // if h.p1 != nil {
        if let Some(ref mut p1) = self.p1 {
            // h.p1.code <<= 1
            p1.code <<= 1;
            // h.p1.codeBits++
            p1.code_bits += 1;
        // } else {
        } else if let Some(ref mut h1) = self.h1 {
            // h.h1.AddZero()
            h1.add_zero();
        // }
        }
    }

    // func (h *PatternHuff) AddOne() {
    pub fn add_one(&mut self) {
        // if h.p0 != nil {
        if let Some(ref mut p0) = self.p0 {
            // h.p0.code <<= 1
            p0.code <<= 1;
            // h.p0.code++
            p0.code += 1;
            // h.p0.codeBits++
            p0.code_bits += 1;
        // } else {
        } else if let Some(ref mut h0) = self.h0 {
            // h.h0.AddOne()
            h0.add_one();
        // }
        }
        // if h.p1 != nil {
        if let Some(ref mut p1) = self.p1 {
            // h.p1.code <<= 1
            p1.code <<= 1;
            // h.p1.code++
            p1.code += 1;
            // h.p1.codeBits++
            p1.code_bits += 1;
        // } else {
        } else if let Some(ref mut h1) = self.h1 {
            // h.h1.AddOne()
            h1.add_one();
        // }
        }
    }

    // Helper function to collect all patterns from the tree
    pub fn collect_patterns(&self, patterns: &mut Vec<Pattern>) {
        if let Some(ref p0) = self.p0 {
            patterns.push((**p0).clone());
        } else if let Some(ref h0) = self.h0 {
            h0.collect_patterns(patterns);
        }
        
        if let Some(ref p1) = self.p1 {
            patterns.push((**p1).clone());
        } else if let Some(ref h1) = self.h1 {
            h1.collect_patterns(patterns);
        }
    }
    
    // func (h *PatternHuff) SetDepth(depth int) {
    pub fn set_depth(&mut self, depth: i32) {
        // if h.p0 != nil {
        if let Some(ref mut p0) = self.p0 {
            // h.p0.depth = depth + 1
            p0.depth = depth + 1;
            // h.p0.uses = 0
            p0.uses = 0;
        // }
        }
        // if h.p1 != nil {
        if let Some(ref mut p1) = self.p1 {
            // h.p1.depth = depth + 1
            p1.depth = depth + 1;
            // h.p1.uses = 0
            p1.uses = 0;
        // }
        }
        // if h.h0 != nil {
        if let Some(ref mut h0) = self.h0 {
            // h.h0.SetDepth(depth + 1)
            h0.set_depth(depth + 1);
        // }
        }
        // if h.h1 != nil {
        if let Some(ref mut h1) = self.h1 {
            // h.h1.SetDepth(depth + 1)
            h1.set_depth(depth + 1);
        // }
        }
    }
}

// ========== Port of compress.go lines 533-572 PositionHuff ==========
// type PositionHuff struct {
//     p0         *Position
//     p1         *Position
//     h0         *PositionHuff
//     h1         *PositionHuff
//     uses       uint64
//     tieBreaker uint64
// }
#[derive(Clone)]
pub struct PositionHuff {
    pub p0: Option<Box<Position>>,         // *Position
    pub p1: Option<Box<Position>>,         // *Position
    pub h0: Option<Box<PositionHuff>>,     // *PositionHuff
    pub h1: Option<Box<PositionHuff>>,     // *PositionHuff
    pub uses: u64,                         // uint64
    pub tie_breaker: u64,                  // uint64
}

impl PositionHuff {
    // func (h *PositionHuff) AddZero() {
    pub fn add_zero(&mut self) {
        // if h.p0 != nil {
        if let Some(ref mut p0) = self.p0 {
            // h.p0.code <<= 1
            p0.code <<= 1;
            // h.p0.codeBits++
            p0.code_bits += 1;
        // } else {
        } else if let Some(ref mut h0) = self.h0 {
            // h.h0.AddZero()
            h0.add_zero();
        // }
        }
        // if h.p1 != nil {
        if let Some(ref mut p1) = self.p1 {
            // h.p1.code <<= 1
            p1.code <<= 1;
            // h.p1.codeBits++
            p1.code_bits += 1;
        // } else {
        } else if let Some(ref mut h1) = self.h1 {
            // h.h1.AddZero()
            h1.add_zero();
        // }
        }
    }

    // func (h *PositionHuff) AddOne() {
    pub fn add_one(&mut self) {
        // if h.p0 != nil {
        if let Some(ref mut p0) = self.p0 {
            // h.p0.code <<= 1
            p0.code <<= 1;
            // h.p0.code++
            p0.code += 1;
            // h.p0.codeBits++
            p0.code_bits += 1;
        // } else {
        } else if let Some(ref mut h0) = self.h0 {
            // h.h0.AddOne()
            h0.add_one();
        // }
        }
        // if h.p1 != nil {
        if let Some(ref mut p1) = self.p1 {
            // h.p1.code <<= 1
            p1.code <<= 1;
            // h.p1.code++
            p1.code += 1;
            // h.p1.codeBits++
            p1.code_bits += 1;
        // } else {
        } else if let Some(ref mut h1) = self.h1 {
            // h.h1.AddOne()
            h1.add_one();
        // }
        }
    }

    // Helper function to collect all positions from the tree
    pub fn collect_positions(&self, positions: &mut Vec<Position>) {
        if let Some(ref p0) = self.p0 {
            positions.push((**p0).clone());
        } else if let Some(ref h0) = self.h0 {
            h0.collect_positions(positions);
        }
        
        if let Some(ref p1) = self.p1 {
            positions.push((**p1).clone());
        } else if let Some(ref h1) = self.h1 {
            h1.collect_positions(positions);
        }
    }
    
    // func (h *PositionHuff) SetDepth(depth int) {
    pub fn set_depth(&mut self, depth: i32) {
        // if h.p0 != nil {
        if let Some(ref mut p0) = self.p0 {
            // h.p0.depth = depth + 1
            p0.depth = depth + 1;
            // h.p0.uses = 0
            p0.uses = 0;
        // }
        }
        // if h.p1 != nil {
        if let Some(ref mut p1) = self.p1 {
            // h.p1.depth = depth + 1
            p1.depth = depth + 1;
            // h.p1.uses = 0
            p1.uses = 0;
        // }
        }
        // if h.h0 != nil {
        if let Some(ref mut h0) = self.h0 {
            // h.h0.SetDepth(depth + 1)
            h0.set_depth(depth + 1);
        // }
        }
        // if h.h1 != nil {
        if let Some(ref mut h1) = self.h1 {
            // h.h1.SetDepth(depth + 1)
            h1.set_depth(depth + 1);
        // }
        }
    }
}

// For BinaryHeap - PatternHuff ordering
impl PartialEq for PatternHuff {
    fn eq(&self, other: &Self) -> bool {
        self.uses == other.uses && self.tie_breaker == other.tie_breaker
    }
}

impl Eq for PatternHuff {}

impl PartialOrd for PatternHuff {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PatternHuff {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse for min-heap behavior
        match other.uses.cmp(&self.uses) {
            Ordering::Equal => other.tie_breaker.cmp(&self.tie_breaker),
            other => other,
        }
    }
}

// For BinaryHeap - PositionHuff ordering
impl PartialEq for PositionHuff {
    fn eq(&self, other: &Self) -> bool {
        self.uses == other.uses && self.tie_breaker == other.tie_breaker
    }
}

impl Eq for PositionHuff {}

impl PartialOrd for PositionHuff {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PositionHuff {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse for min-heap behavior
        match other.uses.cmp(&self.uses) {
            Ordering::Equal => other.tie_breaker.cmp(&self.tie_breaker),
            other => other,
        }
    }
}

// Continuation of compress_with_pattern_candidates - building Huffman trees and final pass
pub fn build_huffman_and_compress(
    cf: &mut File,
    intermediate_path: &std::path::Path,
    pos_map: &HashMap<u64, u64>,
    code2pattern: &mut [Pattern],
    in_count: u64,
    empty_words_count: u64,
) -> Result<()> {
    // ========== Port of parallel_compress.go lines 461-516 Pattern Huffman ==========
    // var patternList PatternList
    let mut pattern_list = Vec::new();
    
    // for _, p := range code2pattern {
    for p in code2pattern.iter() {
        // if p.uses > 0 {
        if p.uses > 0 {
            // patternList = append(patternList, p)
            pattern_list.push(p.clone());
        // }
        }
    // }
    }
    
    // slices.SortFunc(patternList, patternListCmp)
    pattern_list.sort_by(|a, b| {
        if a.uses == b.uses {
            // Reverse bits for tiebreaker
            b.code.reverse_bits().cmp(&a.code.reverse_bits())
        } else {
            a.uses.cmp(&b.uses)
        }
    });
    
    // i := 0
    let mut i = 0;
    // Build Huffman tree for codes
    // var codeHeap PatternHeap
    let mut code_heap: BinaryHeap<PatternHuff> = BinaryHeap::new();
    // tieBreaker := uint64(0)
    let mut tie_breaker = 0u64;
    
    // for codeHeap.Len()+(patternList.Len()-i) > 1 {
    while code_heap.len() + (pattern_list.len() - i) > 1 {
        // New node
        // h := &PatternHuff{
        //     tieBreaker: tieBreaker,
        // }
        let mut h = PatternHuff {
            p0: None,
            p1: None,
            h0: None,
            h1: None,
            uses: 0,
            tie_breaker,
        };
        
        // if codeHeap.Len() > 0 && (i >= patternList.Len() || codeHeap[0].uses < patternList[i].uses) {
        if code_heap.len() > 0 && (i >= pattern_list.len() || code_heap.peek().unwrap().uses < pattern_list[i].uses) {
            // Take h0 from the heap
            // h.h0 = heap.Pop(&codeHeap).(*PatternHuff)
            let mut h0 = code_heap.pop().unwrap();
            // h.h0.AddZero()
            h0.add_zero();
            // h.uses += h.h0.uses
            h.uses += h0.uses;
            h.h0 = Some(Box::new(h0));
        // } else {
        } else {
            // Take p0 from the list
            // h.p0 = patternList[i]
            h.p0 = Some(Box::new(pattern_list[i].clone()));
            // h.p0.code = 0
            h.p0.as_mut().unwrap().code = 0;
            // h.p0.codeBits = 1
            h.p0.as_mut().unwrap().code_bits = 1;
            // h.uses += h.p0.uses
            h.uses += h.p0.as_ref().unwrap().uses;
            // i++
            i += 1;
        // }
        }
        
        // if codeHeap.Len() > 0 && (i >= patternList.Len() || codeHeap[0].uses < patternList[i].uses) {
        if code_heap.len() > 0 && (i >= pattern_list.len() || code_heap.peek().unwrap().uses < pattern_list[i].uses) {
            // Take h1 from the heap
            // h.h1 = heap.Pop(&codeHeap).(*PatternHuff)
            let mut h1 = code_heap.pop().unwrap();
            // h.h1.AddOne()
            h1.add_one();
            // h.uses += h.h1.uses
            h.uses += h1.uses;
            h.h1 = Some(Box::new(h1));
        // } else {
        } else {
            // Take p1 from the list
            // h.p1 = patternList[i]
            h.p1 = Some(Box::new(pattern_list[i].clone()));
            // h.p1.code = 1
            h.p1.as_mut().unwrap().code = 1;
            // h.p1.codeBits = 1
            h.p1.as_mut().unwrap().code_bits = 1;
            // h.uses += h.p1.uses
            h.uses += h.p1.as_ref().unwrap().uses;
            // i++
            i += 1;
        // }
        }
        // tieBreaker++
        tie_breaker += 1;
        // heap.Push(&codeHeap, h)
        code_heap.push(h);
    // }
    }
    
    // if codeHeap.Len() > 0 {
    if code_heap.len() > 0 {
        // root := heap.Pop(&codeHeap).(*PatternHuff)
        let mut root = code_heap.pop().unwrap();
        // root.SetDepth(0)
        root.set_depth(0);
    // }
    }

    // TODO: Continue with position Huffman tree building and writing the final file
    // Lines 567-730
    
    Ok(())
}