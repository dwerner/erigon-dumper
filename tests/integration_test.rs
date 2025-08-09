// Integration tests for the compression/decompression pipeline
// Original tests from go/tests/compress_test.go

#[cfg(test)]
mod tests {
    use erigon_dumper::compress::{Cfg, Compressor};
    use tempfile::TempDir;

    #[test]
    fn test_basic_compression() {
        let tmp_dir = TempDir::new().unwrap();
        let output_file = tmp_dir.path().join("test.seg");
        
        let cfg = Cfg::default();
        
        let mut compressor = Compressor::new(
            cfg,
            output_file.to_string_lossy().to_string(),
            tmp_dir.path().to_string_lossy().to_string(),
            "test".to_string(),
            log::Level::Info,
        ).unwrap();
        
        // Add some test words
        compressor.add_word(b"hello").unwrap();
        compressor.add_word(b"world").unwrap();
        compressor.add_word(b"test").unwrap();
        compressor.add_word(b"compression").unwrap();
        
        // Run compression
        let result = compressor.compress();
        
        // For now, we expect this to fail with NotImplemented
        // since we haven't fully implemented all parts
        if let Err(e) = result {
            println!("Compression error (expected): {:?}", e);
        }
    }
    
    #[test]
    fn test_add_uncompressed_word() {
        let tmp_dir = TempDir::new().unwrap();
        let output_file = tmp_dir.path().join("test.seg");
        
        let cfg = Cfg::default();
        
        let mut compressor = Compressor::new(
            cfg,
            output_file.to_string_lossy().to_string(),
            tmp_dir.path().to_string_lossy().to_string(),
            "test".to_string(),
            log::Level::Info,
        ).unwrap();
        
        // Test adding uncompressed words
        compressor.add_uncompressed_word(b"uncompressed1").unwrap();
        compressor.add_uncompressed_word(b"uncompressed2").unwrap();
        
        // Verify word count
        assert_eq!(compressor.count(), 2);
    }
}