pub mod compress;
pub mod decompress;
pub mod error;
pub mod parallel_compress;
pub mod snapshots;

// Re-export main types
pub use compress::{Cfg, Compressor, DictionaryBuilder, Pattern};
pub use decompress::{Decompressor, Getter};
pub use error::CompressionError;
pub use parallel_compress::{
    compress_with_pattern_candidates, cover_word_by_patterns, CompressionQueue,
};
