/// RecSplit index reader for Erigon snapshot files
/// Based on the Go implementation in erigon-lib/recsplit
use crate::snapshots::{Result, SnapshotError};
use memmap2::Mmap;
use murmur3;
use std::fs::File;
use std::io::Cursor;
use std::path::Path;

/// Features supported in the index file
#[derive(Debug, Clone, Copy)]
pub struct Features(u8);

impl Features {
    pub const NONE: Features = Features(0b0);
    pub const ENUMS: Features = Features(0b1);
    pub const LESS_FALSE_POSITIVES: Features = Features(0b10);

    pub fn contains(&self, feature: Features) -> bool {
        self.0 & feature.0 != 0
    }
}

/// RecSplit index for perfect hash lookup
pub struct RecSplitIndex {
    mmap: Mmap,
    base_data_id: u64,
    key_count: u64,
    bytes_per_rec: u8,
    rec_mask: u64,
    bucket_count: u64,
    bucket_size: u16,
    leaf_size: u16,
    salt: u32,
    start_seed: Vec<u64>,
    features: Features,

    // Offsets into the mmap data
    records_offset: usize,
    bucket_data_offset: usize,
    golomb_rice_offset: usize,

    // For enum indexes - we store the offset and size of the EF data
    offset_ef_start: Option<usize>,
    offset_ef_size: Option<usize>,
}

impl RecSplitIndex {
    /// Open a RecSplit index file
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };

        if mmap.len() < 17 {
            return Err(SnapshotError::InvalidFormat(
                "Index file too small".to_string(),
            ));
        }

        let mut offset = 0;

        // Read header: baseDataID (8) + keyCount (8) + bytesPerRec (1)
        let base_data_id = u64::from_be_bytes(
            mmap[offset..offset + 8]
                .try_into()
                .map_err(|_| SnapshotError::InvalidFormat("Invalid baseDataID".to_string()))?,
        );
        offset += 8;

        let key_count = u64::from_be_bytes(
            mmap[offset..offset + 8]
                .try_into()
                .map_err(|_| SnapshotError::InvalidFormat("Invalid keyCount".to_string()))?,
        );
        offset += 8;

        let bytes_per_rec = mmap[offset];
        offset += 1;

        let rec_mask = (1u64 << (8 * bytes_per_rec)) - 1;
        let records_offset = offset;

        // Skip records
        offset += (key_count as usize) * (bytes_per_rec as usize);

        if offset + 8 > mmap.len() {
            return Err(SnapshotError::InvalidFormat(
                "Index file truncated after records".to_string(),
            ));
        }

        // Read bucket count, bucket size, leaf size
        let bucket_count = u64::from_be_bytes(
            mmap[offset..offset + 8]
                .try_into()
                .map_err(|_| SnapshotError::InvalidFormat("Invalid bucketCount".to_string()))?,
        );
        offset += 8;

        let bucket_size = u16::from_be_bytes(
            mmap[offset..offset + 2]
                .try_into()
                .map_err(|_| SnapshotError::InvalidFormat("Invalid bucketSize".to_string()))?,
        );
        offset += 2;

        let leaf_size = u16::from_be_bytes(
            mmap[offset..offset + 2]
                .try_into()
                .map_err(|_| SnapshotError::InvalidFormat("Invalid leafSize".to_string()))?,
        );
        offset += 2;

        // Salt
        let salt = u32::from_be_bytes(
            mmap[offset..offset + 4]
                .try_into()
                .map_err(|_| SnapshotError::InvalidFormat("Invalid salt".to_string()))?,
        );
        offset += 4;

        // Start seeds
        let start_seed_len = mmap[offset] as usize;
        offset += 1;

        let mut start_seed = Vec::with_capacity(start_seed_len);
        for _ in 0..start_seed_len {
            if offset + 8 > mmap.len() {
                return Err(SnapshotError::InvalidFormat(
                    "Index file truncated in start seeds".to_string(),
                ));
            }
            start_seed.push(u64::from_be_bytes(
                mmap[offset..offset + 8].try_into().unwrap(),
            ));
            offset += 8;
        }

        // Features
        let features = Features(mmap[offset]);
        offset += 1;

        // Handle enum indexes with Elias-Fano offsets
        let (offset_ef_start, offset_ef_size) =
            if features.contains(Features::ENUMS) && key_count > 0 {
                // Read Elias-Fano encoded offsets
                // Format: count (8 bytes) + u (8 bytes) + data (as uint64 array)
                if offset + 16 > mmap.len() {
                    return Err(SnapshotError::InvalidFormat(
                        "Index file truncated in Elias-Fano header".to_string(),
                    ));
                }

                let ef_start = offset;
                let ef_count = u64::from_be_bytes(mmap[offset..offset + 8].try_into().unwrap());
                let ef_u = u64::from_be_bytes(mmap[offset + 8..offset + 16].try_into().unwrap());

                // The Go code reads the data as: data = unsafe.Slice((*uint64)(unsafe.Pointer(&r[16])), (len(r)-16)/uint64Size)
                // After count and u, the remaining data is treated as an array of uint64s
                // We need to calculate how many uint64s are in the data array

                // Calculate l (bits per lower part)
                let l = if ef_count + 1 == 0 || ef_u == 0 {
                    0
                } else {
                    let ratio = ef_u / (ef_count + 1);
                    if ratio == 0 {
                        0
                    } else {
                        63 - ratio.leading_zeros() as u64
                    }
                };

                // Calculate array sizes (following Go's deriveFields)
                let words_lower_bits = ((ef_count + 1) * l + 63) / 64 + 1;
                let words_upper_bits = (ef_count + 1 + (ef_u >> l) + 63) / 64;

                // Jump table calculation from Go's jumpSizeWords()
                let super_q = 1u64 << 14; // 16384
                let super_q_size = 1 + 16; // 1 + qPerSuperQ
                let jump_words = if ef_count == 0 {
                    0
                } else {
                    (1 + (ef_count - 1) / super_q) * super_q_size
                };

                let total_words = words_lower_bits + words_upper_bits + jump_words;
                let data_size = 16 + (total_words * 8) as usize; // 16 for count+u, then uint64 array

                if offset + data_size > mmap.len() {
                    return Err(SnapshotError::InvalidFormat(format!(
                        "Index file truncated in Elias-Fano data: need {} bytes, have {}",
                        data_size,
                        mmap.len() - offset
                    )));
                }

                offset += data_size;

                // Also skip the existence filter if present
                if features.contains(Features::LESS_FALSE_POSITIVES) {
                    if offset + 8 > mmap.len() {
                        return Err(SnapshotError::InvalidFormat(
                            "Index file truncated in existence filter size".to_string(),
                        ));
                    }
                    let existence_size =
                        u64::from_be_bytes(mmap[offset..offset + 8].try_into().unwrap());
                    offset += 8 + existence_size as usize;
                }

                (Some(ef_start), Some(data_size))
            } else {
                (None, None)
            };

        let golomb_rice_offset = offset;
        let bucket_data_offset = offset; // Will be adjusted

        Ok(RecSplitIndex {
            mmap,
            base_data_id,
            key_count,
            bytes_per_rec,
            rec_mask,
            bucket_count,
            bucket_size,
            leaf_size,
            salt,
            start_seed,
            features,
            records_offset,
            bucket_data_offset,
            golomb_rice_offset,
            offset_ef_start,
            offset_ef_size,
        })
    }

    /// Get the number of keys in the index
    pub fn key_count(&self) -> u64 {
        self.key_count
    }

    /// Get the base data ID
    pub fn base_data_id(&self) -> u64 {
        self.base_data_id
    }

    /// Check if this is an enum index
    pub fn is_enum(&self) -> bool {
        self.features.contains(Features::ENUMS)
    }

    /// Ordinal lookup - get offset for the i-th element (0-based)
    /// This is what we need for headers
    pub fn ordinal_lookup(&self, ordinal: u64) -> Option<u64> {
        if ordinal >= self.key_count {
            return None;
        }

        // For enum indexes, we need to decode from Elias-Fano data
        if let (Some(ef_start), Some(_ef_size)) = (self.offset_ef_start, self.offset_ef_size) {
            // Get the offset from the Elias-Fano encoded data
            self.decode_ef_value(ef_start, ordinal)
        } else {
            // For non-enum indexes, read from the records section
            // Each record is `bytes_per_rec` bytes
            let record_offset =
                self.records_offset + (ordinal as usize * self.bytes_per_rec as usize);

            if record_offset + self.bytes_per_rec as usize > self.mmap.len() {
                return None;
            }

            // Read the offset value based on bytes_per_rec
            let offset = match self.bytes_per_rec {
                1 => self.mmap[record_offset] as u64,
                2 => u16::from_be_bytes(
                    self.mmap[record_offset..record_offset + 2]
                        .try_into()
                        .ok()?,
                ) as u64,
                3 => {
                    let mut bytes = [0u8; 4];
                    bytes[1..].copy_from_slice(&self.mmap[record_offset..record_offset + 3]);
                    u32::from_be_bytes(bytes) as u64
                }
                4 => u32::from_be_bytes(
                    self.mmap[record_offset..record_offset + 4]
                        .try_into()
                        .ok()?,
                ) as u64,
                5 => {
                    let mut bytes = [0u8; 8];
                    bytes[3..].copy_from_slice(&self.mmap[record_offset..record_offset + 5]);
                    u64::from_be_bytes(bytes)
                }
                6 => {
                    let mut bytes = [0u8; 8];
                    bytes[2..].copy_from_slice(&self.mmap[record_offset..record_offset + 6]);
                    u64::from_be_bytes(bytes)
                }
                7 => {
                    let mut bytes = [0u8; 8];
                    bytes[1..].copy_from_slice(&self.mmap[record_offset..record_offset + 7]);
                    u64::from_be_bytes(bytes)
                }
                8 => u64::from_be_bytes(
                    self.mmap[record_offset..record_offset + 8]
                        .try_into()
                        .ok()?,
                ),
                _ => return None,
            };

            Some(offset & self.rec_mask)
        }
    }

    /// Decode a value from the Elias-Fano data
    fn decode_ef_value(&self, ef_start: usize, index: u64) -> Option<u64> {
        // Read count and u from the EF header
        let ef_count = u64::from_be_bytes(self.mmap[ef_start..ef_start + 8].try_into().ok()?);
        let ef_u = u64::from_be_bytes(self.mmap[ef_start + 8..ef_start + 16].try_into().ok()?);

        if index > ef_count {
            return None;
        }

        // Calculate l (bits per lower part) - matching Go's deriveFields()
        let l = if ef_count + 1 == 0 || ef_u == 0 {
            0
        } else {
            let ratio = ef_u / (ef_count + 1);
            if ratio == 0 {
                0
            } else {
                63 - ratio.leading_zeros() as u64
            }
        };

        let lower_bits_mask = if l >= 64 { !0u64 } else { (1u64 << l) - 1 };

        // Calculate array boundaries
        let words_lower_bits = ((ef_count + 1) * l + 63) / 64 + 1;
        let words_upper_bits = (ef_count + 1 + (ef_u >> l) + 63) / 64;

        // Jump table calculation
        const SUPER_Q: u64 = 1 << 14; // 16384
        const SUPER_Q_SIZE: u64 = 1 + 16; // 1 + qPerSuperQ
        const Q: u64 = 1 << 8; // 256
        const Q_MASK: u64 = Q - 1;

        let jump_words = if ef_count == 0 {
            0
        } else {
            (1 + (ef_count - 1) / SUPER_Q) * SUPER_Q_SIZE
        };

        // Get the data as u64 array (starting after count and u)
        // The Go code treats this as little-endian uint64 array
        let data_start = ef_start + 16;

        // Read lower bits - matching Go's get() function
        let mut lower = 0u64;
        if l > 0 {
            let lower_bit_pos = index * l;
            let idx64 = (lower_bit_pos / 64) as usize;
            let shift = lower_bit_pos % 64;

            if data_start + (idx64 + 1) * 8 > self.mmap.len() {
                return None;
            }

            lower = u64::from_le_bytes(
                self.mmap[data_start + idx64 * 8..data_start + (idx64 + 1) * 8]
                    .try_into()
                    .ok()?,
            ) >> shift;

            if shift > 0 && idx64 + 1 < words_lower_bits as usize {
                if data_start + (idx64 + 2) * 8 <= self.mmap.len() {
                    let next_word = u64::from_le_bytes(
                        self.mmap[data_start + (idx64 + 1) * 8..data_start + (idx64 + 2) * 8]
                            .try_into()
                            .ok()?,
                    );
                    lower |= next_word << (64 - shift);
                }
            }
        }

        // Get upper bits array start
        let upper_start = data_start + (words_lower_bits as usize) * 8;
        let jump_start = upper_start + (words_upper_bits as usize) * 8;

        // Use jump table to find starting position
        let jump_super_q = (index / SUPER_Q) * SUPER_Q_SIZE;
        let jump_inside_super_q = (index % SUPER_Q) / Q;

        // Read jump values
        let mut jump = 0u64;
        if jump_words > 0 && jump_start + (jump_super_q as usize) * 8 <= self.mmap.len() {
            jump = u64::from_le_bytes(
                self.mmap[jump_start + (jump_super_q as usize) * 8
                    ..jump_start + (jump_super_q as usize + 1) * 8]
                    .try_into()
                    .ok()?,
            );

            // Add the inside-super-q offset
            if jump_inside_super_q > 0 {
                let idx64 = jump_super_q + 1 + (jump_inside_super_q >> 1);
                let shift = 32 * (jump_inside_super_q % 2);
                if jump_start + ((idx64 + 1) as usize) * 8 <= self.mmap.len() {
                    let offset_word = u64::from_le_bytes(
                        self.mmap[jump_start + (idx64 as usize) * 8
                            ..jump_start + ((idx64 + 1) as usize) * 8]
                            .try_into()
                            .ok()?,
                    );
                    let mask = 0xffffffffu64 << shift;
                    jump += (offset_word & mask) >> shift;
                }
            }
        }

        // Find the correct position in upper bits
        let mut curr_word = jump / 64;
        let mut window = if upper_start + ((curr_word + 1) as usize) * 8 <= self.mmap.len() {
            let word = u64::from_le_bytes(
                self.mmap[upper_start + (curr_word as usize) * 8
                    ..upper_start + ((curr_word + 1) as usize) * 8]
                    .try_into()
                    .ok()?,
            );
            word & (!0u64 << (jump % 64))
        } else {
            return None;
        };

        let mut d = (index & Q_MASK) as u32;

        // Skip words until we have enough 1 bits
        while window.count_ones() <= d {
            d -= window.count_ones();
            curr_word += 1;
            if upper_start + ((curr_word + 1) as usize) * 8 > self.mmap.len() {
                return None;
            }
            window = u64::from_le_bytes(
                self.mmap[upper_start + (curr_word as usize) * 8
                    ..upper_start + ((curr_word + 1) as usize) * 8]
                    .try_into()
                    .ok()?,
            );
        }

        // Select the d-th 1 bit in the current window
        let sel = self.select64(window, d as usize);

        // Calculate final value - matching Go's formula
        let val = ((curr_word * 64 + sel as u64 - index) << l) | (lower & lower_bits_mask);

        Some(val)
    }

    /// Select the n-th set bit in a u64 (0-indexed)
    fn select64(&self, word: u64, n: usize) -> usize {
        let mut remaining = n;
        let mut word = word;

        for i in 0..64 {
            if word & 1 != 0 {
                if remaining == 0 {
                    return i;
                }
                remaining -= 1;
            }
            word >>= 1;
        }

        63
    }

    /// Hash-based lookup (more complex, not implemented yet)
    pub fn lookup(&self, key: &[u8]) -> Option<u64> {
        if self.key_count == 0 {
            return None;
        }

        if self.key_count == 1 {
            return Some(0);
        }

        // Calculate hash using murmur3 with salt
        let mut cursor = Cursor::new(key);
        let hash128 = murmur3::murmur3_x64_128(&mut cursor, self.salt).ok()?;

        // Split the 128-bit hash into two 64-bit parts
        let bucket_hash = (hash128 >> 64) as u64;
        let fingerprint = hash128 as u64;

        // This would require implementing the full RecSplit lookup algorithm
        // with Golomb-Rice decoding, which is quite complex
        // For now, return None
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_features() {
        let features = Features::ENUMS;
        assert!(features.contains(Features::ENUMS));
        assert!(!features.contains(Features::LESS_FALSE_POSITIVES));

        let combined = Features(Features::ENUMS.0 | Features::LESS_FALSE_POSITIVES.0);
        assert!(combined.contains(Features::ENUMS));
        assert!(combined.contains(Features::LESS_FALSE_POSITIVES));
    }
}
