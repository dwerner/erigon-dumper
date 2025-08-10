#![no_main]

use libfuzzer_sys::fuzz_target;
use erigon_dumper::compress::{Compressor, Cfg};
use erigon_dumper::decompress::Decompressor;
use tempfile::TempDir;
use std::path::Path;

fuzz_target!(|data: &[u8]| {
    // Need at least 3 bytes: 1 for workers, 1 for pos length, 1 for data
    if data.len() < 3 {
        return;
    }
    
    // Parse input data
    let workers = (data[0] % 8).max(1) as usize; // 1-8 workers
    let pos_len = ((data[1] as usize) % 100).max(1); // 1-100 positions
    
    if data.len() < 2 + pos_len {
        return;
    }
    
    let pos = &data[2..2 + pos_len];
    let x = &data[2 + pos_len..];
    
    if x.is_empty() {
        return;
    }
    
    // Build word list from input data
    let mut words = Vec::new();
    let mut j = 0;
    
    for i in 0..pos.len() {
        if j >= x.len() {
            break;
        }
        
        if pos[i] == 0 {
            continue;
        }
        
        let word_len = (pos[i] as usize * 10).min(255); // Max word length 255
        let next = (j + word_len).min(x.len());
        
        if next > j {
            words.push(x[j..next].to_vec());
            j = next;
        }
    }
    
    // Skip if we have no words
    if words.is_empty() {
        return;
    }
    
    // Set up temporary directory
    let tmp_dir = match TempDir::new() {
        Ok(dir) => dir,
        Err(_) => return,
    };
    
    let file_path = tmp_dir.path().join("compressed.seg");
    
    // Configure compressor
    let mut cfg = Cfg::default();
    cfg.min_pattern_score = 2;
    cfg.workers = workers;
    
    // Compress the words
    let compress_result = (|| -> Result<(), Box<dyn std::error::Error>> {
        let mut compressor = Compressor::new(
            cfg,
            file_path.to_str().unwrap().to_string(),
            tmp_dir.path().to_str().unwrap().to_string(),
            "fuzz".to_string(),
            log::Level::Debug,
        )?;
        
        // Add all words
        for word in &words {
            compressor.add_word(word)?;
        }
        
        // Compress
        compressor.compress()?;
        
        Ok(())
    })();
    
    if compress_result.is_err() {
        return;
    }
    
    // Try to decompress and verify we can read all words
    let decompress_result = (|| -> Result<(), Box<dyn std::error::Error>> {
        let decompressor = Decompressor::new(&file_path)?;
        let mut getter = decompressor.make_getter();
        
        let mut decompressed_words = Vec::new();
        
        while getter.has_next() {
            let (word, _) = getter.next(Vec::new());
            decompressed_words.push(word);
        }
        
        // Verify we got the same number of words
        assert_eq!(words.len(), decompressed_words.len(), 
            "Word count mismatch: expected {}, got {}", 
            words.len(), decompressed_words.len());
        
        // Verify each word matches
        for (i, (original, decompressed)) in words.iter().zip(decompressed_words.iter()).enumerate() {
            assert_eq!(original, decompressed, 
                "Word {} mismatch: expected {:?}, got {:?}", 
                i, original, decompressed);
        }
        
        Ok(())
    })();
    
    // If decompression failed, panic to let the fuzzer know
    if let Err(e) = decompress_result {
        panic!("Decompression failed: {}", e);
    }
});