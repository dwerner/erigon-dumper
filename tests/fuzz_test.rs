use erigon_dumper::compress::{Compressor, Cfg};
use erigon_dumper::decompress::Decompressor;
use proptest::prelude::*;
use std::path::Path;
use tempfile::TempDir;

// Strategy for generating test words
fn word_strategy() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 1..256)
}

// Strategy for generating a collection of words
fn words_strategy() -> impl Strategy<Value = Vec<Vec<u8>>> {
    prop::collection::vec(word_strategy(), 1..100)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]
    
    #[test]
    fn test_compress_decompress_roundtrip(words in words_strategy()) {
        // Initialize logger
        let _ = env_logger::builder().is_test(true).try_init();
        
        // Set up temporary directory
        let tmp_dir = TempDir::new().unwrap();
        let file_path = tmp_dir.path().join("compressed.seg");
        
        // Configure compressor
        let mut cfg = Cfg::default();
        cfg.min_pattern_score = 2;
        cfg.workers = 1;
        
        // Compress the words
        let mut compressor = Compressor::new(
            cfg,
            file_path.to_str().unwrap().to_string(),
            tmp_dir.path().to_str().unwrap().to_string(),
            "proptest".to_string(),
            log::Level::Debug,
        ).unwrap();
        
        // Add all words
        for word in &words {
            compressor.add_word(word).unwrap();
        }
        
        // Compress
        compressor.compress().unwrap();
        drop(compressor); // Ensure file is closed
        
        // Decompress and verify
        let decompressor = Decompressor::new(&file_path).unwrap();
        let mut getter = decompressor.make_getter();
        
        let mut decompressed_words = Vec::new();
        
        while getter.has_next() {
            let (word, _) = getter.next(Vec::new());
            decompressed_words.push(word);
        }
        
        // Verify we got the same words back
        prop_assert_eq!(words.len(), decompressed_words.len(), 
            "Word count mismatch: expected {}, got {}", 
            words.len(), decompressed_words.len());
        
        for (i, (original, decompressed)) in words.iter().zip(decompressed_words.iter()).enumerate() {
            prop_assert_eq!(original, decompressed, 
                "Word {} mismatch", i);
        }
    }
    
    #[test]
    fn test_compress_with_empty_words(
        mut words in words_strategy(),
        empty_positions in prop::collection::vec(0usize..100usize, 0..10)
    ) {
        // Initialize logger
        let _ = env_logger::builder().is_test(true).try_init();
        
        // Insert empty words at random positions
        for &pos in empty_positions.iter().rev() {
            if pos <= words.len() {
                words.insert(pos, vec![]);
            }
        }
        
        // Set up temporary directory
        let tmp_dir = TempDir::new().unwrap();
        let file_path = tmp_dir.path().join("compressed.seg");
        
        // Configure compressor
        let mut cfg = Cfg::default();
        cfg.min_pattern_score = 2;
        cfg.workers = 1;
        
        // Compress the words
        let mut compressor = Compressor::new(
            cfg,
            file_path.to_str().unwrap().to_string(),
            tmp_dir.path().to_str().unwrap().to_string(),
            "proptest".to_string(),
            log::Level::Debug,
        ).unwrap();
        
        // Add all words (including empty ones)
        for word in &words {
            compressor.add_word(word).unwrap();
        }
        
        // Compress
        compressor.compress().unwrap();
        drop(compressor);
        
        // Decompress and verify
        let decompressor = Decompressor::new(&file_path).unwrap();
        let mut getter = decompressor.make_getter();
        
        let mut decompressed_words = Vec::new();
        
        while getter.has_next() {
            let (word, _) = getter.next(Vec::new());
            decompressed_words.push(word);
        }
        
        // Verify we got the same words back
        prop_assert_eq!(words.len(), decompressed_words.len());
        
        for (i, (original, decompressed)) in words.iter().zip(decompressed_words.iter()).enumerate() {
            prop_assert_eq!(original, decompressed, 
                "Word {} mismatch", i);
        }
    }
    
    #[test]
    fn test_compress_with_various_workers(
        words in words_strategy(),
        workers in 1usize..=8
    ) {
        // Initialize logger
        let _ = env_logger::builder().is_test(true).try_init();
        
        // Set up temporary directory
        let tmp_dir = TempDir::new().unwrap();
        let file_path = tmp_dir.path().join("compressed.seg");
        
        // Configure compressor with variable workers
        let mut cfg = Cfg::default();
        cfg.min_pattern_score = 2;
        cfg.workers = workers;
        
        // Compress the words
        let mut compressor = Compressor::new(
            cfg,
            file_path.to_str().unwrap().to_string(),
            tmp_dir.path().to_str().unwrap().to_string(),
            "proptest".to_string(),
            log::Level::Debug,
        ).unwrap();
        
        // Add all words
        for word in &words {
            compressor.add_word(word).unwrap();
        }
        
        // Compress
        compressor.compress().unwrap();
        drop(compressor);
        
        // Decompress and verify
        let decompressor = Decompressor::new(&file_path).unwrap();
        let mut getter = decompressor.make_getter();
        
        let mut decompressed_words = Vec::new();
        
        while getter.has_next() {
            let (word, _) = getter.next(Vec::new());
            decompressed_words.push(word);
        }
        
        // Verify we got the same words back
        prop_assert_eq!(words.len(), decompressed_words.len());
        
        for (i, (original, decompressed)) in words.iter().zip(decompressed_words.iter()).enumerate() {
            prop_assert_eq!(original, decompressed, 
                "Word {} mismatch with {} workers", i, workers);
        }
    }
}

// Specific edge case tests
#[cfg(test)]
mod edge_cases {
    use super::*;
    
    #[test]
    fn test_single_byte_words() {
        let _ = env_logger::builder().is_test(true).try_init();
        
        // Test with single-byte words
        let words: Vec<Vec<u8>> = (0u8..=255).map(|b| vec![b]).collect();
        
        let tmp_dir = TempDir::new().unwrap();
        let file_path = tmp_dir.path().join("compressed.seg");
        
        let mut cfg = Cfg::default();
        cfg.min_pattern_score = 2;
        
        let mut compressor = Compressor::new(
            cfg,
            file_path.to_str().unwrap().to_string(),
            tmp_dir.path().to_str().unwrap().to_string(),
            "test".to_string(),
            log::Level::Debug,
        ).unwrap();
        
        for word in &words {
            compressor.add_word(word).unwrap();
        }
        
        compressor.compress().unwrap();
        drop(compressor);
        
        let decompressor = Decompressor::new(&file_path).unwrap();
        let mut getter = decompressor.make_getter();
        
        let mut decompressed_words = Vec::new();
        
        while getter.has_next() {
            let (word, _) = getter.next(Vec::new());
            decompressed_words.push(word);
        }
        
        assert_eq!(words.len(), decompressed_words.len(), 
            "Length mismatch: expected {} words, got {}", 
            words.len(), decompressed_words.len());
        
        for (i, (original, decompressed)) in words.iter().zip(decompressed_words.iter()).enumerate() {
            assert_eq!(original, decompressed, 
                "Mismatch at index {}: expected {:?}, got {:?}", 
                i, original, decompressed);
        }
    }
    
    #[test]
    fn test_repeated_words() {
        let _ = env_logger::builder().is_test(true).try_init();
        
        // Test with repeated words - should create good patterns
        let base_word = b"repeated_word_test";
        let mut words = Vec::new();
        for _ in 0..100 {
            words.push(base_word.to_vec());
        }
        
        let tmp_dir = TempDir::new().unwrap();
        let file_path = tmp_dir.path().join("compressed.seg");
        
        let mut cfg = Cfg::default();
        cfg.min_pattern_score = 2;
        
        let mut compressor = Compressor::new(
            cfg,
            file_path.to_str().unwrap().to_string(),
            tmp_dir.path().to_str().unwrap().to_string(),
            "test".to_string(),
            log::Level::Debug,
        ).unwrap();
        
        for word in &words {
            compressor.add_word(word).unwrap();
        }
        
        compressor.compress().unwrap();
        drop(compressor);
        
        let decompressor = Decompressor::new(&file_path).unwrap();
        let mut getter = decompressor.make_getter();
        
        let mut decompressed_words = Vec::new();
        
        while getter.has_next() {
            let (word, _) = getter.next(Vec::new());
            decompressed_words.push(word);
        }
        
        assert_eq!(words.len(), decompressed_words.len(), 
            "Length mismatch: expected {} words, got {}", 
            words.len(), decompressed_words.len());
        
        for (i, (original, decompressed)) in words.iter().zip(decompressed_words.iter()).enumerate() {
            assert_eq!(original, decompressed, 
                "Mismatch at index {}: expected {:?}, got {:?}", 
                i, original, decompressed);
        }
    }
}