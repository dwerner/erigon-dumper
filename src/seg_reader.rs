use crate::decompress::{Decompressor, Getter};
use crate::error::CompressionError;
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FileCompression {
    None = 0b00,
    Keys = 0b01,
    Vals = 0b10,
}

impl FileCompression {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "none" | "" => Ok(FileCompression::None),
            "k" => Ok(FileCompression::Keys),
            "v" => Ok(FileCompression::Vals),
            "kv" => Ok(FileCompression::Keys | FileCompression::Vals),
            _ => Err(format!("invalid file compression type: {}", s)),
        }
    }

    pub fn contains(&self, other: FileCompression) -> bool {
        (*self as u8) & (other as u8) != 0
    }
}

impl std::ops::BitOr for FileCompression {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        unsafe { std::mem::transmute((self as u8) | (rhs as u8)) }
    }
}

impl std::fmt::Display for FileCompression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = if *self == (FileCompression::Keys | FileCompression::Vals) {
            "kv"
        } else {
            match *self {
                FileCompression::None => "none",
                FileCompression::Keys => "k",
                FileCompression::Vals => "v",
            }
        };
        write!(f, "{}", s)
    }
}

/// Reader is a decorator on Getter which can auto-use Next/NextUncompressed
/// based on FileCompression passed to constructor
pub struct Reader<'a> {
    getter: Getter<'a>,
    next_value: bool,
    compression: FileCompression,
}

impl<'a> Reader<'a> {
    pub fn new(getter: Getter<'a>, compression: FileCompression) -> Self {
        Self {
            getter,
            next_value: false,
            compression,
        }
    }

    pub fn has_next(&self) -> bool {
        self.getter.has_next()
    }

    pub fn next(&mut self, buf: Vec<u8>) -> (Vec<u8>, u64) {
        let flag = if self.next_value {
            self.next_value = false;
            FileCompression::Vals
        } else {
            self.next_value = true;
            FileCompression::Keys
        };

        if self.compression.contains(flag) {
            self.getter.next(buf)
        } else {
            self.getter.next_uncompressed()
        }
    }

    pub fn reset(&mut self, offset: u64) {
        self.next_value = false;
        self.getter.reset(offset);
    }

    pub fn skip(&mut self) -> (u64, usize) {
        let flag = if self.next_value {
            self.next_value = false;
            FileCompression::Vals
        } else {
            self.next_value = true;
            FileCompression::Keys
        };

        if self.compression.contains(flag) {
            self.getter.skip()
        } else {
            self.getter.skip_uncompressed().unwrap_or((0, 0))
        }
    }

    pub fn match_prefix(&self, prefix: &[u8]) -> bool {
        if self.compression.contains(FileCompression::Keys) {
            self.getter.match_prefix(prefix)
        } else {
            self.getter.match_prefix_uncompressed(prefix)
        }
    }

    pub fn match_cmp(&self, prefix: &[u8]) -> std::cmp::Ordering {
        if self.compression.contains(FileCompression::Keys) {
            self.getter.match_cmp(prefix)
        } else {
            self.getter.match_cmp_uncompressed(prefix)
        }
    }
}

/// SegmentReader provides a higher-level interface for reading segment files
/// with support for different compression types
pub struct SegmentReader {
    decompressor: Decompressor,
    compression: FileCompression,
    file_path: String,
}

impl SegmentReader {
    pub fn new(
        path: impl AsRef<Path>,
        compression: FileCompression,
    ) -> Result<Self, CompressionError> {
        let path = path.as_ref();
        let decompressor = Decompressor::new(path)?;
        let file_path = path.to_string_lossy().to_string();

        Ok(Self {
            decompressor,
            compression,
            file_path,
        })
    }

    pub fn make_reader(&self) -> Reader {
        Reader::new(self.decompressor.make_getter(), self.compression)
    }

    pub fn count(&self) -> usize {
        self.decompressor.count()
    }

    pub fn file_name(&self) -> &str {
        &self.file_path
    }

    pub fn compression(&self) -> FileCompression {
        self.compression
    }
}

/// Detect compression type by attempting to read the file with different modes
pub fn detect_compress_type(decompressor: &Decompressor) -> FileCompression {
    let mut getter = decompressor.make_getter();

    let key_is_compressed = {
        getter.reset(0);
        let mut compressed = false;
        for _ in 0..100 {
            if getter.has_next() {
                if getter.skip_uncompressed().is_err() {
                    compressed = true;
                    break;
                }
            }
            if getter.has_next() {
                getter.skip();
            }
        }
        compressed
    };

    let val_is_compressed = {
        getter.reset(0);
        let mut compressed = false;
        for _ in 0..100 {
            if getter.has_next() {
                getter.skip();
            }
            if getter.has_next() {
                if getter.skip_uncompressed().is_err() {
                    compressed = true;
                    break;
                }
            }
        }
        compressed
    };

    let mut compression = FileCompression::None;
    if key_is_compressed {
        compression = compression | FileCompression::Keys;
    }
    if val_is_compressed {
        compression = compression | FileCompression::Vals;
    }

    getter.reset(0);
    compression
}
