# Erigon Segment Compression - Go Reference Implementation

This directory contains the Go source files from Erigon's segment compression implementation, organized for reference while porting to Rust.

## Directory Structure

```
go/
├── src/                    # Core compression/decompression implementation
│   ├── compress.go         # Main compression logic and dictionary building
│   ├── decompress.go       # Decompression and file reading
│   └── parallel_compress.go # Parallel compression and Huffman encoding
├── tests/                  # Test files
│   ├── compress_test.go    # Compression tests
│   ├── decompress_test.go  # Decompression tests
│   ├── compress_fuzz_test.go   # Compression fuzzing tests
│   └── decompress_fuzz_test.go # Decompression fuzzing tests
└── dependencies/          # Required dependencies
    ├── patricia/          # Patricia tree for pattern matching
    ├── sais/             # Suffix array for pattern discovery
    └── etl/              # Extract-Transform-Load utilities
```

## Algorithm Overview

The Erigon compression uses a **two-stage dictionary-based compression** algorithm:

### Stage 1: Dictionary Building
1. Analyze input data to find repeated patterns using suffix arrays (SAIS)
2. Score patterns based on frequency and compression potential
3. Build an optimal dictionary of patterns using a heap-based selection
4. Organize patterns in a Patricia tree for fast matching

### Stage 2: Compression with Huffman Encoding
1. Match input words against dictionary patterns using Patricia tree
2. Encode matched patterns and their positions using Huffman coding
3. Store unmatched data as "uncovered" bytes
4. Write compressed output with bit-level encoding

## File Format

The compressed file format consists of:

```
[Header]
├── Word Count (8 bytes, big-endian)
├── Empty Words Count (8 bytes, big-endian)
├── Dictionary Size (8 bytes, big-endian)

[Pattern Dictionary]
├── For each pattern:
│   ├── Depth (varint) - Huffman tree depth
│   ├── Length (varint) - Pattern length
│   └── Pattern bytes

[Position Dictionary]
├── Dictionary Size (8 bytes, big-endian)
└── For each position:
    ├── Depth (varint) - Huffman tree depth
    └── Position (varint)

[Compressed Data]
└── Bit-stream of Huffman-encoded positions and patterns
```

## Key Components

### 1. compress.go
- **Compressor struct**: Main compression orchestrator
- **DictionaryBuilder**: Collects and scores pattern candidates
- **Compress()**: Main compression entry point
- Pattern scoring and selection logic

### 2. decompress.go
- **Decompressor struct**: Manages decompression state
- **Getter struct**: Provides word-by-word decompression
- **Next()**: Two-pass decompression algorithm
  - First pass: Places patterns in output buffer
  - Second pass: Fills uncovered gaps with raw data
- Huffman decoding tables for patterns and positions

### 3. parallel_compress.go
- **compressWithPatternCandidates()**: Main compression pipeline
- **coverWordByPatterns()**: Dynamic programming for optimal pattern coverage
- **PatternHuff/PositionHuff**: Huffman tree construction
- **BitWriter**: Bit-level output stream
- Parallel compression worker implementation

### 4. patricia/patricia.go
- **PatriciaTree**: Trie structure for pattern storage
- **MatchFinder2**: Finds all pattern matches in input
- Critical for fast pattern matching during compression

### 5. sais/
- **SAIS (Suffix Array Induced Sorting)**: C implementation with Go bindings
- Finds repeated substrings in input data
- Used during dictionary building to discover patterns

### 6. etl/
- **Collector**: Manages large-scale data collection and sorting
- **Buffer management**: Efficient I/O for large files
- Used for processing pattern candidates

## Compression Algorithm Details

### Pattern Selection
1. Use SAIS to find all repeated substrings
2. Score patterns by: `score = frequency * (length - overhead)`
3. Select top N patterns that maximize compression ratio
4. Minimum pattern length: 5 bytes (configurable)
5. Maximum dictionary size: 64K patterns (configurable)

### Huffman Encoding
- Separate Huffman trees for:
  - **Pattern codes**: Which pattern to use
  - **Position codes**: Where to place patterns
- Variable-length encoding based on frequency
- Canonical Huffman codes for efficient decoding

### Two-Pass Decompression
1. **First Pass**: Read positions and patterns, place patterns in buffer
2. **Second Pass**: Read same positions, fill gaps with uncovered data
3. This allows proper reconstruction without storing gap sizes

## Key Configuration Parameters

```go
type Cfg struct {
    MinPatternScore uint64  // Minimum score for pattern inclusion (default: 1024)
    MinPatternLen   int    // Minimum pattern length (default: 5)
    MaxPatternLen   int    // Maximum pattern length (default: 128)
    MaxDictPatterns int    // Maximum dictionary size (default: 64K)
    SamplingFactor  uint64 // Sample rate for pattern discovery (default: 4)
    Workers         int    // Parallel workers (default: 1)
}
```

## Performance Characteristics

Based on Erigon's benchmarks (74GB uncompressed file):
- Dictionary Size vs Performance:
  - 1M patterns: 35.8GB compressed, 4m06s decompress
  - 64K patterns: 38.6GB compressed, 3m16s decompress
  - 32K patterns: 39.6GB compressed, 3m00s decompress
- Memory usage scales with dictionary size
- Decompression is I/O bound for large files

## Porting Considerations

When porting to Rust:

1. **Patricia Tree**: Can use existing Rust trie crates or port directly
2. **SAIS**: May need FFI to C code or find Rust implementation
3. **Bit I/O**: Rust has good bit manipulation libraries
4. **Parallel Processing**: Use Rust's rayon or tokio
5. **Memory Management**: Rust's ownership helps avoid Go's GC overhead
6. **Error Handling**: Convert Go's error returns to Rust's Result<T, E>

## Testing Strategy

The implementation includes:
- Unit tests for each component
- Fuzz tests for compression/decompression
- Round-trip tests (compress → decompress → verify)
- Pattern matching tests
- Huffman encoding/decoding tests

## Next Steps for Rust Port

1. Implement basic types (Pattern, Position, Config)
2. Port PatriciaTree or use Rust alternative
3. Implement BitWriter/BitReader for Huffman coding
4. Port dictionary building logic
5. Implement compression pipeline
6. Implement two-pass decompression
7. Add comprehensive tests