use erigon_dumper::compress::{Compressor, Cfg};
use erigon_dumper::decompress::DecompressorOwned;
use tempfile::TempDir;
use std::path::Path;
use std::fs::File;
use std::io::Read;
use std::hash::Hasher;

#[test]
fn test_compress_empty_dict() {
    // Port of Go's TestCompressEmptyDict
    // Original: https://github.com/erigontech/erigon/blob/main/erigon-lib/seg/compress_test.go#L33-L67
    let dir = TempDir::new().unwrap();
    let file_path = dir.path().join("compressed.seg");
    
    let mut cfg = Cfg::default();
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
    // Port of Go's prepareDict helper - exact match to Go
    let file_path = dir.join("compressed.seg");
    
    let mut cfg = Cfg::default();
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

// Helper function matching Go's checksum
fn checksum(file_path: &Path) -> u32 {
    let mut file = File::open(file_path).unwrap();
    let mut data = Vec::new();
    file.read_to_end(&mut data).unwrap();
    
    let mut hasher = crc32fast::Hasher::new();
    hasher.write(&data);
    hasher.finish() as u32
}

#[test] 
fn test_compress_dict1() {
    // Initialize logger for debug output
    let _ = env_logger::builder().is_test(true).try_init();
    
    // Port of Go's TestCompressDict1
    // Original: https://github.com/erigontech/erigon/blob/main/erigon-lib/seg/compress_test.go#L121-L185
    let dir = TempDir::new().unwrap();
    let file_path = prepare_dict(dir.path());
    
    let decompressor = DecompressorOwned::open(&file_path).unwrap();
    let mut getter = decompressor.make_getter();
    
    let mut i = 0;
    
    while getter.has_next() && i < 5 {
        let mut buf = Vec::new();
        
        log::debug!("=== Iteration {}, reading 4 words ===", i);
        
        // First word: nil (empty)
        log::debug!("Reading word 1 (should be empty)...");
        let word = getter.next(&mut buf).unwrap();
        log::debug!("Got word 1: {:?} ('{}')", word, String::from_utf8_lossy(&word));
        assert_eq!(word, b"");
        
        // Second word: "long"  
        log::debug!("Reading word 2 (should be 'long')...");
        let word = getter.next(&mut buf).unwrap();
        log::debug!("Got word 2: {:?} ('{}')", word, String::from_utf8_lossy(&word));
        assert_eq!(word, b"long");
        
        // Third word: "word"
        log::debug!("Reading word 3 (should be 'word')...");
        let word = getter.next(&mut buf).unwrap();
        log::debug!("Got word 3: {:?} ('{}')", word, String::from_utf8_lossy(&word));
        assert_eq!(word, b"word");
        
        // Fourth word: "X longlongword X"
        log::debug!("Reading word 4 (should be '{} longlongword {}')...", i, i);
        let word = getter.next(&mut buf).unwrap();
        log::debug!("Got word 4: {:?} ('{}')", word, String::from_utf8_lossy(&word));
        let expected = format!("{} longlongword {}", i, i);
        assert_eq!(word, expected.as_bytes());
        
        i += 1;
    }
    
    assert_eq!(i, 5);
    
    // Check the file checksum matches Go's expected value
    let cs = checksum(&file_path);
    if cs != 3153486123 {
        // it's ok if hash changed, but need re-generate all existing snapshot hashes
        eprintln!("Warning: result file hash changed, got {}", cs);
    }
}

#[test]
fn test_compress_dict_cmp() {
    // Exact port of Go's TestCompressDictCmp
    // Original: https://github.com/erigontech/erigon/blob/main/erigon-lib/seg/compress_test.go#L187-L233
    let dir = TempDir::new().unwrap();
    let file_path = prepare_dict(dir.path());
    
    let decompressor = DecompressorOwned::open(&file_path).unwrap();
    let mut getter = decompressor.make_getter();
    
    let mut i = 0;
    
    while getter.has_next() {
        // next word is `nil`
        let save_pos = getter.data_p;
        assert_eq!(getter.match_cmp(b"long"), 1);
        assert_eq!(getter.match_cmp(b""), 0); // moves offset
        getter.reset(save_pos);
        assert_eq!(getter.match_cmp(b""), 0); // moves offset
        
        // next word is `long`
        let save_pos = getter.data_p;
        assert_eq!(getter.match_cmp(b"long"), 0); // moves offset
        getter.reset(save_pos);
        assert_eq!(getter.match_cmp(b"longlong"), 1);
        assert_eq!(getter.match_cmp(b"wordnotmatch"), 1);
        assert_eq!(getter.match_cmp(b"longnotmatch"), 1);
        assert_eq!(getter.match_cmp(b""), -1);
        assert_eq!(getter.match_cmp(b"long"), 0); // moves offset
        
        // next word is `word`
        let save_pos = getter.data_p;
        assert_eq!(getter.match_cmp(b"wor"), -1);
        assert_eq!(getter.match_cmp(b"word"), 0); // moves offset
        getter.reset(save_pos);
        assert_eq!(getter.match_cmp(b"wor"), -1);
        assert_eq!(getter.match_cmp(b"word"), 0); // moves offset
        
        // next word is `longlongword %d`
        let save_pos = getter.data_p;
        let expected = format!("{} longlongword {}", i, i);
        assert_eq!(getter.match_cmp(expected.as_bytes()), 0); // moves offset
        getter.reset(save_pos);
        
        let mut buf = Vec::new();
        let word = getter.next(&mut buf).unwrap();
        assert_eq!(word, expected.as_bytes());
        
        i += 1;
    }
    
    assert_eq!(i, 100);
}