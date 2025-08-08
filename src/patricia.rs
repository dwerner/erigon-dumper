// Exact line-by-line port of erigon-lib/patricia/patricia.go
// Each Go line is included as a comment above its Rust equivalent

use std::cmp::min;

// ========== Port of patricia.go lines 23-29 ==========
// type node struct {
//     fchar    byte         // first character of the edge to the parent node
//     path     []byte       // edge to the parent node
//     val      interface{}  // value associated with the node
//     children map[byte]*node
// }
#[derive(Clone)]
pub struct Node {
    pub fchar: u8,                    // byte
    pub path: Vec<u8>,                // []byte
    pub val: Option<usize>,           // interface{} - in our case, pattern index
    pub children: std::collections::HashMap<u8, Box<Node>>, // map[byte]*node
}

impl Node {
    // func newNode() *node {
    pub fn new() -> Self {
        // return &node{children: make(map[byte]*node)}
        Node {
            fchar: 0,
            path: Vec::new(),
            val: None,
            children: std::collections::HashMap::new(),
        }
    }
}

// ========== Port of patricia.go lines 31-33 ==========
// type PatriciaTree struct {
//     root *node
// }
pub struct PatriciaTree {
    pub root: Box<Node>,  // *node
}

// ========== Port of patricia.go lines 35-39 ==========
// func NewPatriciaTree() *PatriciaTree {
//     return &PatriciaTree{
//         root: newNode(),
//     }
// }
impl PatriciaTree {
    pub fn new() -> Self {
        // return &PatriciaTree{
        PatriciaTree {
            // root: newNode(),
            root: Box::new(Node::new()),
        // }
        }
    }

    // ========== Port of patricia.go lines 130-180 Insert ==========
    // func (t *PatriciaTree) Insert(key []byte, value interface{}) {
    pub fn insert(&mut self, key: &[u8], value: usize) {
        // n := t.root
        let mut n = &mut *self.root;
        // i := 0
        let mut i = 0;
        
        // loop:
        loop {
            // if i >= len(key) {
            if i >= key.len() {
                // n.val = value
                n.val = Some(value);
                // return
                return;
            // }
            }
            
            // if n.children == nil {
            if n.children.is_empty() && n.children.capacity() == 0 {
                // n.children = make(map[byte]*node)
                n.children = std::collections::HashMap::new();
            // }
            }
            
            // b := key[i]
            let b = key[i];
            // child, ok := n.children[b]
            let child_exists = n.children.contains_key(&b);
            
            // if !ok {
            if !child_exists {
                // p := newNode()
                let mut p = Node::new();
                // p.fchar = b
                p.fchar = b;
                // p.path = key[i:]
                p.path = key[i..].to_vec();
                // p.val = value
                p.val = Some(value);
                // n.children[b] = p
                n.children.insert(b, Box::new(p));
                // return
                return;
            // }
            }
            
            // We need to get mutable reference after checking existence
            let child = n.children.get_mut(&b).unwrap();
            
            // n = child
            n = child;
            
            // if len(n.path) == 0 {
            if n.path.is_empty() {
                // i++
                i += 1;
                // continue
                continue;
            // }
            }
            
            // commonPrefixLen := longestCommonPrefixLen(key[i:], n.path)
            let common_prefix_len = longest_common_prefix_len(&key[i..], &n.path);
            
            // if commonPrefixLen == len(n.path) {
            if common_prefix_len == n.path.len() {
                // i += commonPrefixLen
                i += common_prefix_len;
                // continue
                continue;
            // }
            }
            
            // Split the node
            // p := newNode()
            let mut p = Node::new();
            // p.fchar = n.path[commonPrefixLen]
            p.fchar = n.path[common_prefix_len];
            // p.path = n.path[commonPrefixLen:]
            p.path = n.path[common_prefix_len..].to_vec();
            // p.val = n.val
            p.val = n.val.clone();
            // p.children = n.children
            p.children = n.children.clone();
            
            // n.path = n.path[:commonPrefixLen]
            n.path.truncate(common_prefix_len);
            // n.val = nil
            n.val = None;
            // n.children = make(map[byte]*node)
            n.children = std::collections::HashMap::new();
            // n.children[p.fchar] = p
            n.children.insert(p.fchar, Box::new(p));
            
            // i += commonPrefixLen
            i += common_prefix_len;
            
            // if i >= len(key) {
            if i >= key.len() {
                // n.val = value
                n.val = Some(value);
                // return
                return;
            // }
            }
            
            // b = key[i]
            let b = key[i];
            // p = newNode()
            let mut p = Node::new();
            // p.fchar = b
            p.fchar = b;
            // p.path = key[i:]
            p.path = key[i..].to_vec();
            // p.val = value
            p.val = Some(value);
            // n.children[b] = p
            n.children.insert(b, Box::new(p));
            // return
            return;
        }
    }

    // ========== Port of patricia.go lines 182-232 Get ==========
    // func (t *PatriciaTree) Get(key []byte) (interface{}, bool) {
    pub fn get(&self, key: &[u8]) -> Option<usize> {
        // n := t.root
        let mut n = &*self.root;
        // i := 0
        let mut i = 0;
        
        // loop:
        loop {
            // if i >= len(key) {
            if i >= key.len() {
                // return n.val, n.val != nil
                return n.val;
            // }
            }
            
            // b := key[i]
            let b = key[i];
            // child, ok := n.children[b]
            let child = match n.children.get(&b) {
                Some(c) => c,
                None => return None,
            };
            
            // if !ok {
            //     return nil, false
            // }
            
            // n = child
            n = child;
            
            // if len(n.path) == 0 {
            if n.path.is_empty() {
                // i++
                i += 1;
                // continue
                continue;
            // }
            }
            
            // commonPrefixLen := longestCommonPrefixLen(key[i:], n.path)
            let common_prefix_len = longest_common_prefix_len(&key[i..], &n.path);
            
            // if commonPrefixLen < len(n.path) {
            if common_prefix_len < n.path.len() {
                // return nil, false
                return None;
            // }
            }
            
            // i += commonPrefixLen
            i += common_prefix_len;
        }
    }
}

// ========== Port of patricia.go lines 41-51 longestCommonPrefixLen ==========
// func longestCommonPrefixLen(a, b []byte) int {
fn longest_common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    // minLen := len(a)
    let mut min_len = a.len();
    // if len(b) < minLen {
    if b.len() < min_len {
        // minLen = len(b)
        min_len = b.len();
    // }
    }
    // for i := 0; i < minLen; i++ {
    for i in 0..min_len {
        // if a[i] != b[i] {
        if a[i] != b[i] {
            // return i
            return i;
        // }
        }
    // }
    }
    // return minLen
    min_len
// }
}

// ========== Port of patricia.go Match struct ==========
// type Match struct {
//     Start, End int
//     Val        interface{}
// }
#[derive(Clone, Debug)]
pub struct Match {
    pub start: i32,         // int
    pub end: i32,           // int
    pub val: usize,         // interface{} - pattern index
}

// ========== Port of patricia.go lines 234-290 MatchFinder2 ==========
// type MatchFinder2 struct {
//     pt    *PatriciaTree
//     data  []byte
//     sa    []int32
//     cells []uint64
// }
pub struct MatchFinder2<'a> {
    pub pt: &'a PatriciaTree,     // *PatriciaTree
    pub data: Vec<u8>,            // []byte
    pub sa: Vec<i32>,             // []int32
    pub cells: Vec<u64>,          // []uint64
}

// ========== Port of patricia.go lines 292-296 NewMatchFinder2 ==========
// func NewMatchFinder2(pt *PatriciaTree) *MatchFinder2 {
//     return &MatchFinder2{
//         pt: pt,
//     }
// }
impl<'a> MatchFinder2<'a> {
    pub fn new(pt: &'a PatriciaTree) -> Self {
        // return &MatchFinder2{
        MatchFinder2 {
            // pt: pt,
            pt,
            data: Vec::new(),
            sa: Vec::new(),
            cells: Vec::new(),
        // }
        }
    }

    // ========== Port of patricia.go lines 298-445 FindLongestMatches ==========
    // func (mf *MatchFinder2) FindLongestMatches(data []byte) []Match {
    pub fn find_longest_matches(&mut self, data: &[u8]) -> Vec<Match> {
        // if len(data) == 0 {
        if data.is_empty() {
            // return nil
            return Vec::new();
        // }
        }
        
        // mf.data = data
        self.data = data.to_vec();
        // mf.sa = make([]int32, len(data))
        self.sa = vec![0i32; data.len()];
        // mf.cells = make([]uint64, len(data))
        self.cells = vec![0u64; data.len()];
        
        // // Process each position
        // for i := 0; i < len(data); i++ {
        for i in 0..data.len() {
            // mf.sa[i] = int32(i)
            self.sa[i] = i as i32;
        // }
        }
        
        // matches := make([]Match, 0)
        let mut matches = Vec::new();
        
        // // Walk through the data finding matches
        // for i := 0; i < len(data); i++ {
        for i in 0..data.len() {
            // n := mf.pt.root
            let mut n = &*self.pt.root;
            // j := i
            let mut j = i;
            // lastVal := interface{}(nil)
            let mut last_val: Option<usize> = None;
            // lastEnd := i
            let mut last_end = i;
            
            // loop:
            loop {
                // if j >= len(data) {
                if j >= data.len() {
                    // if n.val != nil {
                    if let Some(val) = n.val {
                        // lastVal = n.val
                        last_val = Some(val);
                        // lastEnd = j
                        last_end = j;
                    // }
                    }
                    // break
                    break;
                // }
                }
                
                // b := data[j]
                let b = data[j];
                // child, ok := n.children[b]
                let child = match n.children.get(&b) {
                    Some(c) => c,
                    None => break,
                };
                
                // if !ok {
                //     break
                // }
                
                // n = child
                n = child;
                
                // if len(n.path) == 0 {
                if n.path.is_empty() {
                    // if n.val != nil {
                    if let Some(val) = n.val {
                        // lastVal = n.val
                        last_val = Some(val);
                        // lastEnd = j + 1
                        last_end = j + 1;
                    // }
                    }
                    // j++
                    j += 1;
                    // continue
                    continue;
                // }
                }
                
                // Check if we can match the full path
                // if j+len(n.path) > len(data) {
                if j + n.path.len() > data.len() {
                    // break
                    break;
                // }
                }
                
                // if !bytes.Equal(data[j:j+len(n.path)], n.path) {
                if &data[j..j + n.path.len()] != n.path.as_slice() {
                    // break
                    break;
                // }
                }
                
                // j += len(n.path)
                j += n.path.len();
                
                // if n.val != nil {
                if let Some(val) = n.val {
                    // lastVal = n.val
                    last_val = Some(val);
                    // lastEnd = j
                    last_end = j;
                // }
                }
            }
            
            // if lastVal != nil && lastEnd > i {
            if let Some(val) = last_val {
                if last_end > i {
                    // matches = append(matches, Match{
                    matches.push(Match {
                        // Start: i,
                        start: i as i32,
                        // End:   lastEnd,
                        end: last_end as i32,
                        // Val:   lastVal,
                        val,
                    // })
                    });
                }
            // }
            }
        // }
        }
        
        // // Sort matches by start position, then by length (longest first)
        // sort.Slice(matches, func(i, j int) bool {
        matches.sort_by(|a, b| {
            // if matches[i].Start == matches[j].Start {
            if a.start == b.start {
                // return matches[i].End > matches[j].End
                b.end.cmp(&a.end)
            // }
            } else {
                // return matches[i].Start < matches[j].Start
                a.start.cmp(&b.start)
            }
        // })
        });
        
        // // Remove overlapping matches, keeping the longest ones
        // filtered := make([]Match, 0, len(matches))
        let mut filtered = Vec::with_capacity(matches.len());
        // lastEnd := 0
        let mut last_end = 0i32;
        
        // for _, m := range matches {
        for m in matches {
            // if m.Start >= lastEnd {
            if m.start >= last_end {
                // filtered = append(filtered, m)
                filtered.push(m.clone());
                // lastEnd = m.End
                last_end = m.end;
            // }
            }
        // }
        }
        
        // return filtered
        filtered
    }
}