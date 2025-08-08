use erigon_dumper::compress::{Compressor, Cfg};
use std::fs;
use std::path::Path;

fn main() {
    let dir = Path::new("/tmp/test_compress");
    fs::create_dir_all(dir).unwrap();
    
    let file_path = dir.join("test.seg");
    let mut cfg = Cfg::default();
    cfg.min_pattern_score = 100; // High score so no patterns
    
    println!("Creating compressor...");
    let mut compressor = Compressor::new("test", &file_path, dir, cfg).unwrap();
    
    println!("Adding word...");
    compressor.add_word(b"word").unwrap();
    
    println!("Compressing...");
    compressor.compress().unwrap();
    
    println!("Closing...");
    compressor.close().unwrap();
    
    // Check file size
    let metadata = fs::metadata(&file_path).unwrap();
    println!("Output file size: {} bytes", metadata.len());
    
    // Read first 100 bytes
    let data = fs::read(&file_path).unwrap();
    println!("First {} bytes (hex):", data.len().min(100));
    for (i, byte) in data.iter().take(100).enumerate() {
        if i % 16 == 0 {
            print!("\n{:04x}: ", i);
        }
        print!("{:02x} ", byte);
    }
    println!();
}