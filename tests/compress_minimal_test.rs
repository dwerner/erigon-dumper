use erigon_dumper::compress::{Compressor, Cfg};
use erigon_dumper::decompress::DecompressorOwned;
use tempfile::TempDir;

#[test]
fn test_minimal_pattern_compression() {
    let dir = TempDir::new().unwrap();
    let file_path = dir.path().join("test.seg");
    
    // Create compressor with very low pattern threshold
    let mut config = Cfg::default();
    config.min_pattern_score = 1;  // Accept all patterns
    
    let mut compressor = Compressor::new(
        "test",
        &file_path,
        dir.path(),
        config,
    ).unwrap();
    
    // Add just 3 words to create a simple pattern
    compressor.add_word(b"").unwrap();      // empty word
    compressor.add_word(b"long").unwrap();  // "long"
    compressor.add_word(b"word").unwrap();  // "word"
    
    // Compress
    compressor.compress().unwrap();
    
    // Now decompress and verify
    let decompressor = DecompressorOwned::open(&file_path).unwrap();
    let mut getter = decompressor.make_getter();
    
    // First word should be empty
    let word = getter.next(&mut Vec::new()).unwrap();
    println!("Word 0: {:?}", String::from_utf8_lossy(&word));
    assert_eq!(word, b"");
    
    // Second word should be "long"
    let word = getter.next(&mut Vec::new()).unwrap();
    println!("Word 1: {:?}", String::from_utf8_lossy(&word));
    assert_eq!(word, b"long");
    
    // Third word should be "word"
    let word = getter.next(&mut Vec::new()).unwrap();
    println!("Word 2: {:?}", String::from_utf8_lossy(&word));
    assert_eq!(word, b"word");
}