# Erigon Dumper Implementation Plan

## Project Overview

Build a Rust library that reads Erigon's historical blockchain data directly from disk to replace Firehose for initial subgraph syncing in graph-node. This will provide 4,000-40,000x performance improvement over JSON-RPC calls.

## Architecture

### Core Components

1. **Segment Reader** (`segment.rs`)
   - Memory-mapped access to .seg and .idx files
   - Zero-copy reading using memmap2 and bytemuck
   - Efficient block range queries

2. **Decompressor** (`decompress.rs`)
   - Dictionary-based compression algorithm from Erigon
   - Pattern matching and position encoding
   - Direct operation on byte slices (no temp files)

3. **Block Reader** (`reader.rs`)
   - High-level API for reading blocks and headers
   - Handles decompression and RLP decoding
   - Converts between Alloy consensus and RPC types

4. **Data Transformer** (pending)
   - Convert Erigon data to graph-node compatible format
   - Match Firehose protocol expectations

## Implementation Phases

### Phase 1: Foundation ✅
- [x] Project setup with Cargo.toml
- [x] Basic type definitions using bytemuck
- [x] Error handling structure
- [x] Segment file reader skeleton
- [x] Basic decompression algorithm (no dictionary)

### Phase 2: Core Decompression 🚧
- [x] Bit-level reading operations
- [x] Position encoding/decoding
- [x] Uncompressed data handling
- [ ] Dictionary parsing from segment headers
- [ ] Pattern table implementation
- [ ] Full dictionary-based decompression

### Phase 3: Segment File Support 📋
- [ ] Parse .idx files for block indices
- [ ] Parse .seg files with proper headers
- [ ] Handle multiple segment files per range
- [ ] Efficient block lookup by number/hash
- [ ] Transaction and receipt parsing

### Phase 4: Graph-Node Integration 📋
- [ ] Implement Firehose protocol interface
- [ ] Create block streaming API
- [ ] Add cursor/checkpoint support
- [ ] Handle chain reorganizations
- [ ] Create gRPC service wrapper

### Phase 5: Production Readiness 📋
- [ ] Performance benchmarks
- [ ] Memory usage optimization
- [ ] Comprehensive error handling
- [ ] Integration tests with real data
- [ ] Documentation and examples

## Technical Decisions

### Why Rust?
- Zero-cost abstractions for performance
- Memory safety without GC overhead
- Excellent FFI for graph-node integration
- Strong ecosystem (Alloy, memmap2, bytemuck)

### Why Zero-Copy?
- Erigon files can be large (GBs)
- Avoid memory allocation overhead
- Direct disk-to-consumer data flow
- Leverage OS page cache

### Executor Agnostic Design
- No tokio/async-std dependencies
- Library can be used in any runtime
- Simpler testing with smol_potat
- Better composability

## Performance Targets

Based on napkin math calculations:
- Direct disk read: ~50-500 MB/s
- Target: 10,000+ blocks/second
- Memory usage: O(1) regardless of data size
- Startup time: <100ms

## Testing Strategy

1. **Unit Tests**
   - Compression/decompression roundtrip
   - Bit operations correctness
   - Edge cases (empty blocks, large blocks)

2. **Integration Tests**
   - Real Erigon segment files
   - Full block reconstruction
   - Performance benchmarks

3. **Graph-Node Tests**
   - Protocol compatibility
   - Data format validation
   - Streaming performance

## Dependencies

### Core
- `alloy-*`: Ethereum types and RLP
- `memmap2`: Memory-mapped files
- `bytemuck`: Zero-copy casting
- `thiserror`: Error handling

### Testing
- `criterion`: Benchmarking
- `smol-potat`: Minimal async runtime
- `tempfile`: Test file handling

## Known Challenges

1. **Dictionary Format**: Erigon's compression format is undocumented
2. **Segment Evolution**: Format may change between Erigon versions
3. **Large Files**: Some segments can be several GB
4. **Cursor Complexity**: Handling forks and reorgs

## Future Enhancements

1. **Parallel Processing**: Read multiple segments concurrently
2. **Caching Layer**: LRU cache for frequently accessed blocks
3. **Compression**: Support for newer Erigon compression algorithms
4. **State Access**: Direct state trie reading for eth_call support

## Success Criteria

1. Successfully read blocks from Erigon segment files
2. Match or exceed Firehose performance for initial sync
3. Pass graph-node integration tests
4. Handle mainnet data volumes efficiently
5. Maintain <100MB memory usage under load

## References

- Erigon Compression: https://github.com/erigontech/erigon-lib/tree/main/compress
- Firehose Protocol: https://github.com/streamingfast/firehose-ethereum
- Graph-Node: https://github.com/graphprotocol/graph-node
- Alloy Types: https://github.com/alloy-rs/alloy