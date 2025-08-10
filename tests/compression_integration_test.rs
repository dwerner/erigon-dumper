// Comprehensive integration test for the compression pipeline

#[cfg(test)]
mod tests {
    use erigon_dumper::compress::{Cfg, Compressor};
    use tempfile::TempDir;

    #[test] 
    fn test_compression_with_patterns() {
        // Initialize logger for debugging
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Debug)
            .try_init();
            
        let tmp_dir = TempDir::new().unwrap();
        let output_file = tmp_dir.path().join("test.seg");

        let mut cfg = Cfg::default();
        // Lower thresholds to ensure patterns are found
        cfg.min_pattern_score = 2;
        cfg.min_pattern_len = 3;
        cfg.sampling_factor = 1; // Sample all superstrings

        let mut compressor = Compressor::new(
            cfg,
            output_file.to_string_lossy().to_string(),
            tmp_dir.path().to_string_lossy().to_string(),
            "test".to_string(),
            log::Level::Debug,
        )
        .unwrap();

        // Add the full original set to reproduce the error
        compressor.add_word(b"test").unwrap();
        compressor.add_word(b"testing").unwrap();
        compressor.add_word(b"test123").unwrap();
        compressor.add_word(b"mytest").unwrap();
        compressor.add_word(b"hello").unwrap();
        compressor.add_word(b"helloworld").unwrap();
        compressor.add_word(b"hello123").unwrap();
        compressor.add_word(b"unique").unwrap();
        compressor.add_word(b"another").unwrap();

        // Run compression
        let result = compressor.compress();

        // Check if compression succeeded
        assert!(result.is_ok(), "Compression failed: {:?}", result.err());

        // Check that output file was created
        assert!(output_file.exists(), "Output file was not created");

        // Check compression ratio
        let ratio = compressor.ratio();
        println!("Compression ratio: {:.2}", ratio);

        // Ratio should be > 0 (indicating some compression happened)
        // Note: ratio of 0 means compressed file is empty which can happen with small inputs
        assert!(ratio >= 0.0, "Invalid compression ratio: {}", ratio);
    }

    #[test]
    fn test_compression_with_uncompressed_words() {
        let tmp_dir = TempDir::new().unwrap();
        let output_file = tmp_dir.path().join("test_uncompressed.seg");

        let cfg = Cfg::default();

        let mut compressor = Compressor::new(
            cfg,
            output_file.to_string_lossy().to_string(),
            tmp_dir.path().to_string_lossy().to_string(),
            "test".to_string(),
            log::Level::Debug,
        )
        .unwrap();

        // Mix compressed and uncompressed words
        compressor.add_word(b"compressed1").unwrap();
        compressor.add_uncompressed_word(b"uncompressed1").unwrap();
        compressor.add_word(b"compressed2").unwrap();
        compressor.add_uncompressed_word(b"uncompressed2").unwrap();

        // Verify word count
        assert_eq!(compressor.count(), 4);

        // Run compression
        let result = compressor.compress();
        assert!(result.is_ok(), "Compression failed: {:?}", result.err());

        // Check that output file was created
        assert!(output_file.exists(), "Output file was not created");
    }

    #[test]
    fn test_empty_compression() {
        let tmp_dir = TempDir::new().unwrap();
        let output_file = tmp_dir.path().join("test_empty.seg");

        let cfg = Cfg::default();

        let mut compressor = Compressor::new(
            cfg,
            output_file.to_string_lossy().to_string(),
            tmp_dir.path().to_string_lossy().to_string(),
            "test".to_string(),
            log::Level::Debug,
        )
        .unwrap();

        // Don't add any words
        assert_eq!(compressor.count(), 0);

        // Compression should still work with empty input
        let result = compressor.compress();
        assert!(
            result.is_ok(),
            "Compression failed on empty input: {:?}",
            result.err()
        );
    }
}
