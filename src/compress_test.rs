// Port of compress_test.go
// Original: go/tests/compress_test.go

#[cfg(test)]
mod tests {
    use crate::compress::*;
    use tempfile::TempDir;

    // Go test: TestCompressEmptyDict
    #[test]
    fn test_compress_empty_dict() {
        use crate::decompress::Decompressor;

        let tmp_dir = TempDir::new().unwrap();
        let file_path = tmp_dir.path().join("compressed");

        let mut cfg = Cfg::default();
        cfg.min_pattern_score = 100;

        let mut compressor = Compressor::new(
            cfg,
            file_path.to_string_lossy().to_string(),
            tmp_dir.path().to_string_lossy().to_string(),
            "test".to_string(),
            log::Level::Debug,
        )
        .unwrap();

        // Now we can test AddWord
        compressor.add_word(b"word").unwrap();

        // Compress the data
        compressor.compress().unwrap();
        drop(compressor); // Close by dropping

        // Test decompression
        let decompressor = Decompressor::new(&file_path).unwrap();
        let mut getter = decompressor.make_getter();

        assert!(getter.has_next());
        let (word, _) = getter.next(Vec::new());
        assert_eq!(word, b"word");
        assert!(!getter.has_next());
    }

    // Go test: prepareDict helper
    fn prepare_dict() -> Vec<Vec<u8>> {
        let mut words = Vec::new();

        // From Go: compress_test.go:97-108
        for _i in 0..100 {
            words.push(Vec::new()); // nil word
        }

        for _i in 0..100 {
            words.push(format!("longlongword").into_bytes());
        }

        for _i in 0..10 {
            words.push(format!("veryveryverylongword").into_bytes());
        }

        for i in 0..200 {
            words.push(format!("word{}", i).into_bytes());
        }

        for i in 0..30 {
            words.push(format!("longlongword{}", i).into_bytes());
        }

        for i in 0..10 {
            words.push(format!("superword{}longlongword", i).into_bytes());
        }

        for i in 0..10 {
            words.push(format!("superword{}longlongword{}", i, i).into_bytes());
        }

        for i in 0..10 {
            words.push(format!("{}longlongword", i).into_bytes());
        }

        words
    }

    // Go test: TestCompressDict1
    #[test]
    fn test_compress_dict1() {
        use crate::decompress::Decompressor;

        let tmp_dir = TempDir::new().unwrap();
        let file_path = tmp_dir.path().join("compressed");

        let mut cfg = Cfg::default();
        cfg.min_pattern_score = 1;
        cfg.workers = 2;

        let mut compressor = Compressor::new(
            cfg,
            file_path.to_string_lossy().to_string(),
            tmp_dir.path().to_string_lossy().to_string(),
            "test".to_string(),
            log::Level::Debug,
        )
        .unwrap();

        let words = prepare_dict();

        // Add all words to compressor
        for word in &words {
            compressor.add_word(word).unwrap();
        }
        compressor.compress().unwrap();
        drop(compressor); // Close by dropping

        // Decompress and verify
        let decompressor = Decompressor::new(&file_path).unwrap();
        let mut getter = decompressor.make_getter();

        for expected_word in &words {
            assert!(getter.has_next());
            let (word, _) = getter.next(Vec::new());
            assert_eq!(word, expected_word.as_slice());
        }
        assert!(!getter.has_next());
    }

    // Test for DictionaryBuilder (not in original Go tests, but useful)
    #[test]
    fn test_dictionary_builder_operations() {
        let mut builder = DictionaryBuilder::new(100);

        // Add some patterns
        builder.process_word(b"hello".to_vec(), 50);
        builder.process_word(b"world".to_vec(), 100);
        builder.process_word(b"test".to_vec(), 75);

        assert_eq!(builder.len(), 3);

        // Test finish with hard limit
        builder.finish(2);
        assert_eq!(builder.len(), 2);

        // Verify the patterns are sorted by score
        let mut collected = Vec::new();
        builder.for_each(|score, word| {
            collected.push((score, word.to_vec()));
        });

        // Should have kept the highest scoring patterns
        assert_eq!(collected.len(), 2);
        assert!(collected.iter().any(|(s, _)| *s == 100));
        assert!(collected.iter().any(|(s, _)| *s == 75));
        assert!(!collected.iter().any(|(s, _)| *s == 50));
    }

    // Test Pattern ordering for heap operations
    #[test]
    fn test_pattern_heap_ordering() {
        use std::collections::BinaryHeap;

        let mut heap = BinaryHeap::new();

        heap.push(Pattern::new(b"pattern1".to_vec(), 100));
        heap.push(Pattern::new(b"pattern2".to_vec(), 50));
        heap.push(Pattern::new(b"pattern3".to_vec(), 200));
        heap.push(Pattern::new(b"pattern4".to_vec(), 75));

        // BinaryHeap is a max-heap, but our Ord implementation reverses it
        // So we should get patterns in ascending score order when popping
        let p1 = heap.pop().unwrap();
        assert_eq!(p1.score, 50);

        let p2 = heap.pop().unwrap();
        assert_eq!(p2.score, 75);

        let p3 = heap.pop().unwrap();
        assert_eq!(p3.score, 100);

        let p4 = heap.pop().unwrap();
        assert_eq!(p4.score, 200);
    }

    // Test CompressionWord
    #[test]
    fn test_compression_word() {
        let word = CompressionWord::new(b"test".to_vec(), 42);
        assert_eq!(word.word, b"test");
        assert_eq!(word.order, 42);
    }

    // Test Ring initialization
    #[test]
    fn test_ring_new() {
        let ring = Ring::new();
        assert_eq!(ring.count, 0);
        assert_eq!(ring.head, 0);
        assert_eq!(ring.tail, 0);
        assert_eq!(ring.cells.len(), 16);
    }

    // Test Cfg default values match Go defaults
    #[test]
    fn test_cfg_defaults() {
        let cfg = Cfg::default();
        assert_eq!(cfg.min_pattern_score, 1024);
        assert_eq!(cfg.min_pattern_len, 5);
        assert_eq!(cfg.max_pattern_len, 128);
        assert_eq!(cfg.sampling_factor, 4);
        assert_eq!(cfg.max_dict_patterns, 64 * 1024);
        assert_eq!(cfg.dict_reducer_soft_limit, 1_000_000);
        assert_eq!(cfg.workers, 1);
    }
}
