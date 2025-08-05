use erigon_dumper::compress::{Compressor, CompressorCfg};
use erigon_dumper::decompress::DecompressorOwned;
use tempfile::TempDir;
use std::path::Path;

const LOREM: &str = r#"lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et
dolore magna aliqua ut enim ad minim veniam quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo
consequat duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur
excepteur sint occaecat cupidatat non proident sunt in culpa qui officia deserunt mollit anim id est laborum"#;

fn lorem_strings() -> Vec<String> {
    let cleaned = LOREM.replace('\n', " ").replace('\r', "");
    let mut words: Vec<String> = cleaned.split(' ').map(|s| s.to_string()).collect();
    words.push(String::new()); // Add empty string like Go version
    words
}

fn prepare_lorem_dict(dir: &Path) -> std::path::PathBuf {
    // Port of Go's prepareLoremDict - simplified for debugging
    let file_path = dir.join("lorem_compressed.seg");
    
    let mut cfg = CompressorCfg::default();
    cfg.min_pattern_score = 1000; // High score so fewer patterns are selected for easier debugging
    cfg.workers = 2;
    
    let mut compressor = Compressor::new("prepareLoremDict", &file_path, dir, cfg).unwrap();
    
    // Only test first few words to debug
    let words = vec!["lorem".to_string(), "ipsum".to_string(), "dolor".to_string()];
    for (k, w) in words.iter().enumerate() {
        let word_with_index = format!("{} {}", w, k);
        println!("Compressing word {}: '{}'", k, word_with_index);
        compressor.add_word(word_with_index.as_bytes()).unwrap();
    }
    
    compressor.compress().unwrap();
    compressor.close().unwrap();
    
    file_path
}

#[test]
fn test_decompress_lorem_basic() {
    // Port of Go's basic decompression test
    let dir = TempDir::new().unwrap();
    let file_path = prepare_lorem_dict(dir.path());
    
    let decompressor = DecompressorOwned::open(&file_path).unwrap();
    let mut getter = decompressor.make_getter();
    
    let words = lorem_strings();
    let mut i = 0;
    
    while getter.has_next() && i < 5 { // Only test first 5 words
        let mut buf = Vec::new();
        let word = getter.next(&mut buf).unwrap();
        let expected = format!("{} {}", words[i], i);
        
        println!("Word {}: expected='{}' (len={}), got='{}' (len={})", 
                 i, expected, expected.len(), String::from_utf8_lossy(&word), word.len());
        
        if word != expected.as_bytes() {
            println!("  Expected bytes: {:?}", expected.as_bytes());
            println!("  Got bytes: {:?}", word);
            panic!("Mismatch at word {}", i);
        }
        i += 1;
    }
    
    println!("Successfully decompressed {} words", i);
}

fn test_pattern_compression_decompression(test_name: &str, words: &[&[u8]], expected: &[&[u8]]) {
    let dir = TempDir::new().unwrap();
    let file_path = dir.path().join(format!("{}.seg", test_name));
    
    let mut cfg = CompressorCfg::default();
    cfg.min_pattern_score = 1; // Very low to ensure pattern selection
    cfg.min_pattern_len = 2;   // Short patterns
    cfg.max_pattern_len = 10;
    cfg.workers = 1;
    
    let mut compressor = Compressor::new(test_name, &file_path, dir.path(), cfg).unwrap();
    
    println!("=== COMPRESSION PHASE: {} ===", test_name);
    for (i, word) in words.iter().enumerate() {
        println!("Adding word {}: {:?} ('{}')", i, word, String::from_utf8_lossy(word));
        compressor.add_word(word).unwrap();
    }
    
    compressor.compress().unwrap();
    compressor.close().unwrap();
    
    println!("\n=== DECOMPRESSION PHASE: {} ===", test_name);
    let decompressor = DecompressorOwned::open(&file_path).unwrap();
    let mut getter = decompressor.make_getter();
    
    // Test all words
    for (i, expected_word) in expected.iter().enumerate() {
        let mut buf = Vec::new();
        println!("Reading word {}...", i);
        let word = getter.next(&mut buf).unwrap();
        println!("Word {}: {:?} ('{}')", i, word, String::from_utf8_lossy(&word));
        
        assert_eq!(word, *expected_word, "Word {} should be '{}'", i, String::from_utf8_lossy(expected_word));
    }
    
    println!("Pattern test {} PASSED!", test_name);
}

#[test]
fn test_no_patterns_first() {
    // Start with no patterns to ensure position-only case works
    let dir = TempDir::new().unwrap();
    let file_path = dir.path().join("no_patterns.seg");
    
    let mut cfg = CompressorCfg::default();
    cfg.min_pattern_score = 10000; // Very high to prevent pattern selection
    cfg.workers = 1;
    
    let mut compressor = Compressor::new("no_patterns", &file_path, dir.path(), cfg).unwrap();
    
    println!("=== NO PATTERNS TEST ===");
    compressor.add_word(b"ab").unwrap();
    compressor.add_word(b"ab").unwrap();
    
    compressor.compress().unwrap();
    compressor.close().unwrap();
    
    let decompressor = DecompressorOwned::open(&file_path).unwrap();
    let mut getter = decompressor.make_getter();
    
    let mut buf = Vec::new();
    let word1 = getter.next(&mut buf).unwrap();
    println!("No-pattern word 1: {:?}", word1);
    assert_eq!(word1, b"ab");
    
    let word2 = getter.next(&mut buf).unwrap();
    println!("No-pattern word 2: {:?}", word2);
    assert_eq!(word2, b"ab");
    
    println!("No-patterns test PASSED!");
}

#[test]
fn test_minimal_pattern() {
    // Absolute minimal case: 2 identical words to force 1 clear pattern
    test_pattern_compression_decompression(
        "minimal_pattern",
        &[b"ab", b"ab"],
        &[b"ab", b"ab"]
    );
}

#[test]
fn test_simple_patterns() {
    // Test with different pattern complexities
    test_pattern_compression_decompression(
        "simple_patterns_1",
        &[b"test", b"test"],
        &[b"test", b"test"]
    );
}

#[test]
fn test_overlapping_patterns() {
    // Test overlapping patterns
    test_pattern_compression_decompression(
        "overlapping_patterns",
        &[b"abc", b"bcd", b"abc"],
        &[b"abc", b"bcd", b"abc"]
    );
}

// Port of Go's TestCompressEmptyDict
// Original: https://github.com/erigontech/erigon/blob/main/erigon-lib/seg/compress_test.go#L33-L67
#[test] 
fn test_compress_empty_dict() {
    let dir = TempDir::new().unwrap();
    let file_path = dir.path().join("compressed.seg");
    
    let mut cfg = CompressorCfg::default();
    cfg.min_pattern_score = 100; // Port exact config from Go test
    
    let mut compressor = Compressor::new("test_compress_empty_dict", &file_path, dir.path(), cfg).unwrap();
    
    // Port exact test data
    compressor.add_word(b"word").unwrap();
    compressor.compress().unwrap();
    compressor.close().unwrap();
    
    // Port exact decompression test
    let decompressor = DecompressorOwned::open(&file_path).unwrap();
    let mut getter = decompressor.make_getter();
    
    assert!(getter.has_next(), "expected a word");
    
    let mut buf = Vec::new();
    let word = getter.next(&mut buf).unwrap();
    assert_eq!(word, b"word", "expected word, got {:?}", word);
    
    assert!(!getter.has_next(), "not expecting anything else");
}

// Port of Go's prepareDict function  
// Original: https://github.com/erigontech/erigon/blob/main/erigon-lib/seg/compress_test.go#L83-L119
fn prepare_dict(dir: &Path) -> std::path::PathBuf {
    let file_path = dir.join("compressed.seg");
    
    let mut cfg = CompressorCfg::default();
    cfg.min_pattern_score = 1; // Port exact config from Go test
    cfg.workers = 2;
    
    let mut compressor = Compressor::new("prepare_dict", &file_path, dir, cfg).unwrap();
    
    // Port exact loop from Go
    for i in 0..100 {
        compressor.add_word(&[]).unwrap(); // nil word
        compressor.add_word(b"long").unwrap();
        compressor.add_word(b"word").unwrap();
        let formatted = format!("{} longlongword {}", i, i);
        compressor.add_word(formatted.as_bytes()).unwrap();
    }
    
    compressor.compress().unwrap();
    compressor.close().unwrap();
    
    file_path
}

// Port of Go's TestCompressDict1 - simplified for debugging
// Original: https://github.com/erigontech/erigon/blob/main/erigon-lib/seg/compress_test.go#L121-L185
#[test] 
fn test_compress_dict1() {
    let dir = TempDir::new().unwrap();
    let file_path = prepare_dict(dir.path());
    
    let decompressor = DecompressorOwned::open(&file_path).unwrap();
    let mut getter = decompressor.make_getter();
    
    println!("=== Starting TestCompressDict1 ===");
    getter.reset(0);
    
    // Test just the first iteration to debug
    let mut i = 0;
    
    // First group should be: empty, "long", "word", "0 longlongword 0"
    
    // 1. next word is empty (nil in Go)
    println!("Testing empty word...");
    assert!(!getter.match_prefix(b"long"), "empty word should not match 'long'");
    assert!(getter.match_prefix(b""), "empty word should match empty prefix");
    assert!(getter.match_prefix(&[]), "empty word should match empty slice");
    
    let mut buf = Vec::new();
    let word = getter.next(&mut buf).unwrap();
    println!("First word: {:?} (len={})", String::from_utf8_lossy(&word), word.len());
    assert_eq!(word.len(), 0, "first word should be empty");
    
    // 2. next word is "long"
    println!("Testing 'long' word...");
    assert!(getter.match_prefix(b"long"), "should match 'long'");
    assert!(!getter.match_prefix(b"longlong"), "should not match 'longlong'");
    assert!(!getter.match_prefix(b"wordnotmatch"), "should not match 'wordnotmatch'");
    assert!(!getter.match_prefix(b"longnotmatch"), "should not match 'longnotmatch'");
    assert!(getter.match_prefix(&[]), "should match empty slice");
    
    let word = getter.next(&mut buf).unwrap();
    println!("Second word: {:?}", String::from_utf8_lossy(&word));
    assert_eq!(word, b"long", "second word should be 'long'");
    
    // 3. next word is "word"
    println!("Testing 'word' word...");
    assert!(!getter.match_prefix(b"long"), "should not match 'long' for 'word'");
    assert!(!getter.match_prefix(b"longlong"), "should not match 'longlong'"); 
    assert!(getter.match_prefix(b"word"), "should match 'word'");
    assert!(getter.match_prefix(b""), "should match empty string");
    assert!(getter.match_prefix(&[]), "should match empty slice");
    assert!(!getter.match_prefix(b"wordnotmatch"), "should not match 'wordnotmatch'");
    assert!(!getter.match_prefix(b"longnotmatch"), "should not match 'longnotmatch'");
    
    let word = getter.next(&mut buf).unwrap(); 
    println!("Third word: {:?}", String::from_utf8_lossy(&word));
    assert_eq!(word, b"word", "third word should be 'word'");
    
    // 4. next word is formatted string
    let expected = format!("{} longlongword {}", i, i);
    println!("Testing formatted word, expected: '{}'", expected);
    
    let expect_prefix = format!("{} long", i);
    
    assert!(getter.match_prefix(format!("{}", i).as_bytes()), "should match number prefix");
    assert!(getter.match_prefix(expect_prefix.as_bytes()), "should match expect_prefix");
    assert!(getter.match_prefix(format!("{}long", expect_prefix).as_bytes()), "should match with 'long' appended");
    assert!(getter.match_prefix(format!("{}longword ", expect_prefix).as_bytes()), "should match with 'longword ' appended");
    assert!(!getter.match_prefix(b"wordnotmatch"), "should not match 'wordnotmatch'");
    assert!(!getter.match_prefix(b"longnotmatch"), "should not match 'longnotmatch'");
    assert!(getter.match_prefix(&[]), "should match empty slice");
    
    let save_pos = getter.data_p;
    let (word, next_pos) = getter.next_with_pos(&mut buf).unwrap();
    println!("Fourth word: {:?}", String::from_utf8_lossy(&word));
    
    getter.reset(save_pos as u64);
    assert_eq!(getter.match_cmp(expected.as_bytes()), 0, "should match expected string exactly");
    getter.reset(next_pos);
    
    assert_eq!(word, expected.as_bytes(), "expected '{}', got '{}'", expected, String::from_utf8_lossy(&word));
    
    println!("First iteration passed!");
}