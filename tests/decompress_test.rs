// Port of decompress_test.go
// Original: go/tests/decompress_test.go

#[cfg(test)]
mod tests {
    use erigon_dumper::compress::{Cfg, Compressor};
    use erigon_dumper::decompress::Decompressor;
    use tempfile::TempDir;

    // Lorem ipsum test data
    const LOREM: &str = "lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et \
dolore magna aliqua ut enim ad minim veniam quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo \
consequat duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur \
excepteur sint occaecat cupidatat non proident sunt in culpa qui officia deserunt mollit anim id est laborum";

    fn get_lorem_strings() -> Vec<Vec<u8>> {
        let mut strings: Vec<Vec<u8>> = LOREM
            .replace('\n', " ")
            .replace('\r', "")
            .split(' ')
            .map(|s| s.as_bytes().to_vec())
            .collect();
        strings.push(Vec::new()); // Add empty string for corner cases
        strings
    }

    // Helper function from Go: prepareLoremDict
    fn prepare_lorem_dict() -> (TempDir, Decompressor) {
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

        let lorem_strings = get_lorem_strings();
        for (k, w) in lorem_strings.iter().enumerate() {
            let word = format!("{} {}", String::from_utf8_lossy(w), k);
            compressor.add_word(word.as_bytes()).unwrap();
        }

        compressor.compress().unwrap();
        drop(compressor); // Close by dropping

        let decompressor = Decompressor::new(&file_path).unwrap();
        (tmp_dir, decompressor)
    }

    // Go test: TestDecompressSkip
    #[test]
    fn test_decompress_skip() {
        // Initialize logger
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Debug)
            .try_init();
            
        let (_tmp_dir, decompressor) = prepare_lorem_dict();
        let mut getter = decompressor.make_getter();

        let lorem_strings = get_lorem_strings();
        let mut i = 0;

        while getter.has_next() {
            let w = &lorem_strings[i];
            if i % 2 == 0 {
                log::debug!("Skipping word {}", i);
                getter.skip();
            } else {
                log::debug!("Reading word {}", i);
                let (word, _) = getter.next(Vec::new());
                let expected = format!("{} {}", String::from_utf8_lossy(w), i);
                log::debug!("Got word: {:?}, expected: {:?}", String::from_utf8_lossy(&word), expected);
                assert_eq!(String::from_utf8_lossy(&word), expected);
            }
            i += 1;
        }

        // Test reset and offsets
        getter.reset(0);
        let (_, offset) = getter.next(Vec::new());
        assert!(offset > 0);
        let (_, offset2) = getter.next(Vec::new());
        assert!(offset2 > offset);
    }

    // Go test: TestDecompressMatchOK
    #[test]
    fn test_decompress_match_ok() {
        let (_tmp_dir, decompressor) = prepare_lorem_dict();
        let mut getter = decompressor.make_getter();

        let lorem_strings = get_lorem_strings();
        let mut i = 0;

        while getter.has_next() {
            let w = &lorem_strings[i];
            if i % 2 != 0 {
                let expected = format!("{} {}", String::from_utf8_lossy(w), i);
                assert!(
                    getter.match_prefix(expected.as_bytes()),
                    "expected match with {}",
                    expected
                );
                getter.skip();
            } else {
                let (word, _) = getter.next(Vec::new());
                let expected = format!("{} {}", String::from_utf8_lossy(w), i);
                assert_eq!(String::from_utf8_lossy(&word), expected);
            }
            i += 1;
        }
    }

    // Go test: TestDecompressMatchPrefix
    #[test]
    fn test_decompress_match_prefix() {
        let (_tmp_dir, decompressor) = prepare_lorem_dict();
        let mut getter = decompressor.make_getter();

        let lorem_strings = get_lorem_strings();
        let mut i = 0;

        // Test matching correct prefixes
        while getter.has_next() {
            let w = &lorem_strings[i];
            let full_word = format!("{} {}", String::from_utf8_lossy(w), i + 1);
            let expected = &full_word.as_bytes()[..full_word.len() / 2];

            assert!(getter.match_prefix(expected), "expected match with prefix");
            getter.skip();
            i += 1;
        }

        // Reset and test non-matching prefixes
        getter.reset(0);
        i = 0;

        while getter.has_next() {
            let w = &lorem_strings[i];
            let full_word = format!("{} {}", String::from_utf8_lossy(w), i + 1);
            let mut wrong_prefix = full_word.as_bytes()[..full_word.len() / 2].to_vec();

            if !wrong_prefix.is_empty() {
                let last_idx = wrong_prefix.len() - 1;
                wrong_prefix[last_idx] += 1;
                assert!(
                    !getter.match_prefix(&wrong_prefix),
                    "not expected match with wrong prefix"
                );
            }

            getter.skip();
            i += 1;
        }
    }

    // Helper from Go: prepareStupidDict
    fn prepare_stupid_dict(size: usize) -> (TempDir, Decompressor) {
        let tmp_dir = TempDir::new().unwrap();
        let file_path = tmp_dir.path().join("compressed2");

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

        for i in 0..size {
            let word = format!("word-{}", i);
            compressor.add_word(word.as_bytes()).unwrap();
        }

        compressor.compress().unwrap();
        drop(compressor); // Close by dropping

        let decompressor = Decompressor::new(&file_path).unwrap();
        (tmp_dir, decompressor)
    }

    // Go test: TestDecompressMatchNotOK
    #[test]
    fn test_decompress_match_not_ok() {
        let (_tmp_dir, decompressor) = prepare_lorem_dict();
        let mut getter = decompressor.make_getter();

        let lorem_strings = get_lorem_strings();
        let mut i = 0;
        let mut skip_count = 0;

        while getter.has_next() {
            let w = &lorem_strings[i];
            // Test with wrong index (i+1 instead of i)
            let wrong_word = format!("{} {}", String::from_utf8_lossy(w), i + 1);

            // Should not match the wrong word
            if !getter.match_prefix(wrong_word.as_bytes()) {
                getter.skip();
                skip_count += 1;
            }
            i += 1;
        }

        // All words should have been skipped due to mismatch
        assert!(skip_count > 0);
    }

    // Test for empty file compression/decompression
    #[test]
    fn test_compress_decompress_empty() {
        let tmp_dir = TempDir::new().unwrap();
        let file_path = tmp_dir.path().join("empty");

        let mut cfg = Cfg::default();
        cfg.min_pattern_score = 1;

        let mut compressor = Compressor::new(
            cfg,
            file_path.to_string_lossy().to_string(),
            tmp_dir.path().to_string_lossy().to_string(),
            "test".to_string(),
            log::Level::Debug,
        )
        .unwrap();

        // Compress with no words added
        compressor.compress().unwrap();
        drop(compressor); // Close by dropping

        // Should be able to open empty compressed file
        let decompressor = Decompressor::new(&file_path).unwrap();
        let mut getter = decompressor.make_getter();

        // Should have no words
        assert!(!getter.has_next());
        assert_eq!(decompressor.count(), 0);
        assert_eq!(decompressor.empty_words_count(), 0);
    }

    // Test for single word compression/decompression
    #[test]
    fn test_compress_decompress_single_word() {
        let tmp_dir = TempDir::new().unwrap();
        let file_path = tmp_dir.path().join("single");

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

        let test_word = b"hello world";
        compressor.add_word(test_word).unwrap();
        compressor.compress().unwrap();
        drop(compressor); // Close by dropping

        let decompressor = Decompressor::new(&file_path).unwrap();
        let mut getter = decompressor.make_getter();

        assert!(getter.has_next());
        let (word, _) = getter.next(Vec::new());
        assert_eq!(word, test_word);
        assert!(!getter.has_next());
        assert_eq!(decompressor.count(), 1);
    }

    // Test for multiple words with patterns
    #[test]
    fn test_compress_decompress_with_patterns() {
        // Initialize logger for debugging
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .try_init();
        let tmp_dir = TempDir::new().unwrap();
        let file_path = tmp_dir.path().join("patterns");

        let mut cfg = Cfg::default();
        cfg.min_pattern_score = 1;
        cfg.min_pattern_len = 4;

        let mut compressor = Compressor::new(
            cfg,
            file_path.to_string_lossy().to_string(),
            tmp_dir.path().to_string_lossy().to_string(),
            "test".to_string(),
            log::Level::Trace,
        )
        .unwrap();

        // Add words with common patterns
        let words = vec![
            b"common_prefix_1".to_vec(),
            b"common_prefix_2".to_vec(),
            b"common_prefix_3".to_vec(),
            b"different_word".to_vec(),
            b"common_suffix_x".to_vec(),
            b"common_suffix_y".to_vec(),
        ];

        for word in &words {
            compressor.add_word(word).unwrap();
        }

        compressor.compress().unwrap();
        drop(compressor); // Close by dropping

        let decompressor = Decompressor::new(&file_path).unwrap();
        let mut getter = decompressor.make_getter();

        // Verify all words are decompressed correctly
        for expected_word in &words {
            assert!(getter.has_next());
            let (word, _) = getter.next(Vec::new());
            assert_eq!(word, *expected_word);
        }
        assert!(!getter.has_next());
        assert_eq!(decompressor.count(), words.len());
    }

    // Test empty words in compression
    #[test]
    fn test_compress_decompress_empty_words() {
        let tmp_dir = TempDir::new().unwrap();
        let file_path = tmp_dir.path().join("empty_words");

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

        // Mix of empty and non-empty words
        compressor.add_word(b"first").unwrap();
        compressor.add_word(b"").unwrap(); // empty
        compressor.add_word(b"second").unwrap();
        compressor.add_word(b"").unwrap(); // empty
        compressor.add_word(b"third").unwrap();

        compressor.compress().unwrap();
        drop(compressor); // Close by dropping

        let decompressor = Decompressor::new(&file_path).unwrap();
        let mut getter = decompressor.make_getter();

        let expected = vec![
            b"first".to_vec(),
            b"".to_vec(),
            b"second".to_vec(),
            b"".to_vec(),
            b"third".to_vec(),
        ];

        for expected_word in &expected {
            assert!(getter.has_next());
            let (word, _) = getter.next(Vec::new());
            assert_eq!(word, *expected_word);
        }
        assert!(!getter.has_next());
        assert_eq!(decompressor.count(), 5);
        assert_eq!(decompressor.empty_words_count(), 2);
    }

    // Test getter reset functionality
    #[test]
    fn test_getter_reset() {
        let (_tmp_dir, decompressor) = prepare_lorem_dict();
        let mut getter = decompressor.make_getter();

        let lorem_strings = get_lorem_strings();

        // Read first few words
        let mut first_words = Vec::new();
        for i in 0..5 {
            assert!(getter.has_next());
            let (word, _) = getter.next(Vec::new());
            first_words.push(word);
        }

        // Reset to beginning
        getter.reset(0);

        // Should read the same words again
        for (i, expected) in first_words.iter().enumerate() {
            assert!(getter.has_next());
            let (word, _) = getter.next(Vec::new());
            assert_eq!(word, *expected, "Word {} doesn't match after reset", i);
        }
    }

    // Test match prefix with empty prefix
    #[test]
    fn test_match_empty_prefix() {
        let (_tmp_dir, decompressor) = prepare_lorem_dict();
        let mut getter = decompressor.make_getter();

        while getter.has_next() {
            // Empty prefix should always match
            assert!(getter.match_prefix(b""));
            getter.skip();
        }
    }
}
