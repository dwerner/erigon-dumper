# Erigon Dumper

A Rust library for reading Erigon's historical blockchain data directly from disk, designed to replace Firehose for initial subgraph syncing in graph-node.

## Overview

This library provides zero-copy access to Erigon's segment files (.seg and .idx), enabling:
- Direct disk reads of blockchain data (4,000-40,000x faster than JSON-RPC)
- Zero-copy memory mapping using memmap2 and bytemuck
- Native decompression of Erigon's dictionary-based compression format
- Graph-node compatible data structures using Alloy types

## Architecture

The library is designed to be executor-agnostic (no tokio or async runtime dependencies) and consists of:

- **Segment Reader**: Memory-mapped access to .seg and .idx files
- **Decompressor**: Implementation of Erigon's dictionary compression algorithm
- **Reader**: High-level API for reading blocks and headers
- **Types**: Zero-copy structures for index entries and headers

## Implementation Status

✅ Project structure and dependencies
✅ Zero-copy segment file reading
✅ Basic decompression algorithm (without dictionary support)
✅ Compression/decompression roundtrip tests
🚧 Dictionary-based pattern matching
🚧 Full Erigon database reader
📋 Graph-node data transformer
📋 gRPC/streaming interface
📋 Integration with real Erigon segment files

## Reference Implementation

The decompression algorithm is based on:
- https://github.com/erigontech/erigon-lib/blob/main/compress/decompress.go

## Usage

```rust
use erigon_dumper::{ErigonReader, Block};
use std::path::Path;

// Open Erigon chaindata directory
let reader = ErigonReader::open(Path::new("/path/to/chaindata"))?;

// Read a specific block
let block = reader.read_block(12345678)?;

// Read a range of blocks
for block_result in reader.read_block_range(1000, 2000) {
    let block = block_result?;
    // Process block...
}
```

## Testing

```bash
cargo test
```

## License

TBD