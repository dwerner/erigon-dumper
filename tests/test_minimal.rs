use erigon_dumper::compress::{Compressor, Cfg};
use erigon_dumper::decompress::DecompressorOwned;
use tempfile::TempDir;

#[test]
fn test_minimal_compression() {
    // Initialize logger for debug output
    let _ = env_logger::builder().is_test(true).try_init();
    let dir = TempDir::new().unwrap();
    let file_path = dir.path().join("test.seg");
    
    let mut cfg = Cfg::default();
    cfg.min_pattern_score = 1;
    cfg.workers = 1;
    
    let mut compressor = Compressor::new("test", &file_path, dir.path(), cfg).unwrap();
    
    // Add just a few words to test
    compressor.add_word(&[]).unwrap();
    compressor.add_word(b"long").unwrap();
    compressor.add_word(b"word").unwrap();
    compressor.add_word(b"0 longlongword 0").unwrap();
    
    compressor.compress().unwrap();
    compressor.close().unwrap();
    
    let decompressor = DecompressorOwned::open(&file_path).unwrap();
    let mut getter = decompressor.make_getter();
    
    let mut i = 0;
    while getter.has_next() {
        let mut buf = Vec::new();
        let word = getter.next(&mut buf).unwrap();
        println!("Word {}: {:?}", i, std::str::from_utf8(&word).unwrap_or("<non-utf8>"));
        i += 1;
    }
    
    assert_eq!(i, 4, "Expected 4 words");
}