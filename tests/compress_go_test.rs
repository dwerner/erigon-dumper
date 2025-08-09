// Exact line-by-line port of Go's compress_test.go

use erigon_dumper::compress::{Compressor, Cfg};
use erigon_dumper::decompress::DecompressorOwned;
use tempfile::TempDir;
use std::path::Path;

// ========== Port of compress_test.go lines 33-67 TestCompressEmptyDict ==========
// func TestCompressEmptyDict(t *testing.T) {
#[test]
fn test_compress_empty_dict() {
    // Initialize logger for debug output
    let _ = env_logger::builder().is_test(true).try_init();
    
    // logger := log.New()
    // Note: We don't have logging in Rust tests by default
    
    // tmpDir := t.TempDir()
    let tmp_dir = TempDir::new().unwrap();
    
    // file := filepath.Join(tmpDir, "compressed")
    let file = tmp_dir.path().join("compressed");
    
    // cfg := DefaultCfg
    let mut cfg = Cfg::default();
    
    // cfg.MinPatternScore = 100
    cfg.min_pattern_score = 100;
    
    // c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, cfg, log.LvlDebug, logger)
    let mut c = Compressor::new(
        "test_compress_empty_dict",  // t.Name()
        &file,                        // file
        tmp_dir.path(),              // tmpDir
        cfg,                         // cfg
    ).unwrap();
    
    // if err != nil {
    //     t.Fatal(err)
    // }
    // Note: unwrap() handles this
    
    // defer c.Close()
    // Note: Will be handled by Drop in Rust
    
    // if err = c.AddWord([]byte("word")); err != nil {
    //     t.Fatal(err)
    // }
    c.add_word(b"word").unwrap();
    
    // if err = c.Compress(); err != nil {
    //     t.Fatal(err)
    // }
    c.compress().unwrap();
    
    // var d *Decompressor
    // if d, err = NewDecompressor(file); err != nil {
    //     t.Fatal(err)
    // }
    let d = DecompressorOwned::open(&file).unwrap();
    
    // defer d.Close()
    // Note: Will be handled by Drop in Rust
    
    // g := d.MakeGetter()
    let mut g = d.make_getter();
    
    // if !g.HasNext() {
    //     t.Fatalf("expected a word")
    // }
    assert!(g.has_next(), "expected a word");
    
    // word, _ := g.Next(nil)
    let mut buf = Vec::new();
    let word = g.next(&mut buf).unwrap();
    
    // if string(word) != "word" {
    //     t.Fatalf("expeced word, got (hex) %x", word)
    // }
    assert_eq!(word, b"word", "expected word, got {:?}", word);
    
    // if g.HasNext() {
    //     t.Fatalf("not expecting anything else")
    // }
    assert!(!g.has_next(), "not expecting anything else");
}

// Let me also port a test with patterns
// ========== Port of a simple pattern test ==========
#[test]
fn test_compress_with_dict() {
    // Initialize logger for debug output
    let _ = env_logger::builder().is_test(true).try_init();
    
    // Create a test similar to Go but simpler
    let tmp_dir = TempDir::new().unwrap();
    let file = tmp_dir.path().join("compressed");
    
    let mut cfg = Cfg::default();
    cfg.min_pattern_score = 1;  // Accept all patterns
    
    let mut c = Compressor::new(
        "test_compress_with_dict",
        &file,
        tmp_dir.path(),
        cfg,
    ).unwrap();
    
    // Add words that will create patterns
    c.add_word(b"").unwrap();        // empty
    c.add_word(b"long").unwrap();    // "long"
    c.add_word(b"longer").unwrap();  // "longer" - contains "long"
    c.add_word(b"word").unwrap();    // "word"
    
    c.compress().unwrap();
    
    // Decompress and verify
    let d = DecompressorOwned::open(&file).unwrap();
    let mut g = d.make_getter();
    
    let mut buf = Vec::new();
    
    // First word: empty
    assert!(g.has_next());
    let word = g.next(&mut buf).unwrap();
    assert_eq!(word, b"");
    
    // Second word: "long"
    assert!(g.has_next());
    let word = g.next(&mut buf).unwrap();
    assert_eq!(word, b"long");
    
    // Third word: "longer"
    assert!(g.has_next());
    let word = g.next(&mut buf).unwrap();
    assert_eq!(word, b"longer");
    
    // Fourth word: "word"
    assert!(g.has_next());
    let word = g.next(&mut buf).unwrap();
    assert_eq!(word, b"word");
    
    assert!(!g.has_next());
}