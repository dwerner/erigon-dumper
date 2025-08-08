// Exact line-by-line port of erigon-lib/seg/dict.go DictionaryBuilder
// Each Go line is included as a comment above its Rust equivalent

use std::collections::{HashMap, BinaryHeap};
use std::cmp::Ordering;
use crate::compress_go_port::Pattern;

// ========== Port of dict.go lines 35-42 ==========
// type DictionaryBuilder struct {
//     lastWord         []byte
//     items            []*Pattern
//     limit            int
//     lastWordScore    uint64
// }
pub struct DictionaryBuilder {
    pub last_word: Vec<u8>,        // []byte
    pub items: Vec<Pattern>,       // []*Pattern
    pub limit: usize,              // int
    pub last_word_score: u64,      // uint64
}

// ========== Port of dict.go lines 44-52 ==========
// func NewDictionaryBuilder(limit int) *DictionaryBuilder {
//     return &DictionaryBuilder{
//         items: make([]*Pattern, 0, limit),
//         limit: limit,
//     }
// }
impl DictionaryBuilder {
    pub fn new(limit: usize) -> Self {
        // return &DictionaryBuilder{
        DictionaryBuilder {
            // items: make([]*Pattern, 0, limit),
            items: Vec::with_capacity(limit),
            // limit: limit,
            limit,
            last_word: Vec::new(),
            last_word_score: 0,
        // }
        }
    }

    // ========== Port of dict.go lines 54-76 ==========
    // func (db *DictionaryBuilder) AddWord(word []byte) error {
    pub fn add_word(&mut self, word: &[u8]) -> Result<(), String> {
        // if bytes.Equal(word, db.lastWord) {
        if word == self.last_word.as_slice() {
            // db.lastWordScore++
            self.last_word_score += 1;
            // return nil
            return Ok(());
        // }
        }
        
        // if len(db.lastWord) > 0 {
        if !self.last_word.is_empty() {
            // db.items = append(db.items, &Pattern{
            self.items.push(Pattern {
                // word:  common.Copy(db.lastWord),
                word: self.last_word.clone(),
                // score: db.lastWordScore,
                score: self.last_word_score,
                uses: 0,
                code: 0,
                code_bits: 0,
                depth: 0,
            // })
            });
        // }
        }
        
        // db.lastWord = common.Copy(word)
        self.last_word = word.to_vec();
        // db.lastWordScore = 1
        self.last_word_score = 1;
        // return nil
        Ok(())
    }

    // ========== Port of dict.go lines 78-87 ==========
    // func (db *DictionaryBuilder) Finish() {
    pub fn finish(&mut self) {
        // if len(db.lastWord) > 0 {
        if !self.last_word.is_empty() {
            // db.items = append(db.items, &Pattern{
            self.items.push(Pattern {
                // word:  db.lastWord,
                word: self.last_word.clone(),
                // score: db.lastWordScore,
                score: self.last_word_score,
                uses: 0,
                code: 0,
                code_bits: 0,
                depth: 0,
            // })
            });
        // }
        }
    }

    // ========== Port of dict.go lines 89-98 ==========
    // func (db *DictionaryBuilder) Sort() {
    pub fn sort(&mut self) {
        // sort.SliceStable(db.items, func(i, j int) bool {
        self.items.sort_by(|a, b| {
            // if db.items[i].score == db.items[j].score {
            if a.score == b.score {
                // return bytes.Compare(db.items[i].word, db.items[j].word) < 0
                a.word.cmp(&b.word)
            // }
            } else {
                // return db.items[i].score > db.items[j].score
                b.score.cmp(&a.score)  // Reverse for descending order
            }
        // })
        });
    }

    // ========== Port of dict.go lines 100-105 ==========
    // func (db *DictionaryBuilder) Limit() {
    pub fn limit(&mut self) {
        // if db.limit > 0 && len(db.items) > db.limit {
        if self.limit > 0 && self.items.len() > self.limit {
            // db.items = db.items[:db.limit]
            self.items.truncate(self.limit);
        // }
        }
    }

    // ========== Port of dict.go lines 107-112 ==========
    // func (db *DictionaryBuilder) ForEach(f func(score uint64, word []byte)) {
    pub fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(u64, &[u8]),
    {
        // for _, item := range db.items {
        for item in &self.items {
            // f(item.score, item.word)
            f(item.score, &item.word);
        // }
        }
    }

    // ========== Port of dict.go lines 114-116 ==========
    // func (db *DictionaryBuilder) Close() {}
    pub fn close(&self) {
        // No-op in Go version
    }

    // ========== Port of dict.go lines 118-120 ==========
    // func (db *DictionaryBuilder) Len() int { return len(db.items) }
    pub fn len(&self) -> usize {
        // return len(db.items)
        self.items.len()
    }

    // ========== Port of dict.go lines 122-129 ==========
    // func (db *DictionaryBuilder) Pattern(i int) *Pattern {
    pub fn pattern(&self, i: usize) -> Option<&Pattern> {
        // if i >= len(db.items) {
        if i >= self.items.len() {
            // return nil
            return None;
        // }
        }
        // return db.items[i]
        Some(&self.items[i])
    }

    // ========== Port of dict.go lines 131-133 ==========
    // func (db *DictionaryBuilder) Patterns() []*Pattern { return db.items }
    pub fn patterns(&self) -> &[Pattern] {
        // return db.items
        &self.items
    }
}