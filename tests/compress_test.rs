use erigon_dumper::compress::{Compressor, CompressorCfg};
use erigon_dumper::decompress::DecompressorOwned;
use tempfile::TempDir;
use std::path::Path;

#[test]
fn test_compress_empty_dict() {
    // Port of Go's TestCompressEmptyDict
    // Original: https://github.com/erigontech/erigon/blob/main/erigon-lib/seg/compress_test.go#L33-L67
    let dir = TempDir::new().unwrap();
    let file_path = dir.path().join("compressed.seg");
    
    let mut cfg = CompressorCfg::default();
    cfg.min_pattern_score = 100; // High score so no patterns are selected
    
    let mut compressor = Compressor::new("test", &file_path, dir.path(), cfg).unwrap();
    
    compressor.add_word(b"word").unwrap();
    compressor.compress().unwrap();
    compressor.close().unwrap();
    
    // Now decompress and verify
    let decompressor = DecompressorOwned::open(&file_path).unwrap();
    let mut getter = decompressor.make_getter();
    
    assert!(getter.has_next());
    let mut buf = Vec::new();
    let word = getter.next(&mut buf).unwrap();
    assert_eq!(word, b"word");
    assert!(!getter.has_next());
}

fn prepare_dict(dir: &Path) -> std::path::PathBuf {
    // Port of Go's prepareDict helper
    // Original: https://github.com/erigontech/erigon/blob/main/erigon-lib/seg/compress_test.go#L83-L119
    let file_path = dir.join("compressed.seg");
    
    let mut cfg = CompressorCfg::default();
    cfg.min_pattern_score = 1; // Low score so many patterns are selected
    cfg.workers = 2;
    
    let mut compressor = Compressor::new("prepareDict", &file_path, dir, cfg).unwrap();
    
    for i in 0..100 {
        compressor.add_word(&[]).unwrap(); // nil in Go
        compressor.add_word(b"long").unwrap();
        compressor.add_word(b"word").unwrap();
        let long_word = format!("{} longlongword {}", i, i);
        compressor.add_word(long_word.as_bytes()).unwrap();
    }
    
    compressor.compress().unwrap();
    compressor.close().unwrap();
    
    file_path
}

#[test] 
fn test_compress_dict1() {
    // Port of Go's TestCompressDict1
    // Original: https://github.com/erigontech/erigon/blob/main/erigon-lib/seg/compress_test.go#L121-L185
    let dir = TempDir::new().unwrap();
    let file_path = prepare_dict(dir.path());
    
    let decompressor = DecompressorOwned::open(&file_path).unwrap();
    let mut getter = decompressor.make_getter();
    
    let mut i = 0;
    getter.reset(0);
    
    while getter.has_next() {
        // First word should be empty (nil in Go)
        let mut buf = Vec::new();
        let word = getter.next(&mut buf).unwrap();
        assert_eq!(word.len(), 0);
        
        // Second word should be "long"
        assert!(getter.has_next());
        let word = getter.next(&mut buf).unwrap();
        assert_eq!(word, b"long");
        
        // Third word should be "word"
        assert!(getter.has_next());
        let word = getter.next(&mut buf).unwrap();
        assert_eq!(word, b"word");
        
        // Fourth word should be formatted string
        assert!(getter.has_next());
        let word = getter.next(&mut buf).unwrap();
        let expected = format!("{} longlongword {}", i, i);
        assert_eq!(word, expected.as_bytes());
        
        i += 1;
    }
    
    assert_eq!(i, 100);
}