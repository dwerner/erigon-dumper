# Erigon Seg File Consumer Patterns

This document describes how Erigon consumes and deserializes .seg files, providing guidance for the Rust port.

## Key Consumer Files Copied

- `src/decompress_full.go` - Full decompressor implementation from Erigon
- `src/seg_reader.go` - Reader wrapper for key-value iteration
- `consumers/block_reader.go` - Shows how blockchain data is read
- `consumers/cat_snapshot.go` - Command-line tool for inspecting seg files
- `consumers/domain_usage.go` - State domain access patterns

## Core Consumption API

### 1. Opening a Seg File

```go
// Basic pattern
d, err := seg.NewDecompressor(filepath)
if err != nil {
    return err
}
defer d.Close()

// Create getter for reading
g := d.MakeGetter()
```

### 2. Getter Interface (Primary Access)

The `Getter` is the main interface for consuming decompressed data:

```go
type Getter struct {
    // Internal state for decompression
}

// Core methods:
func (g *Getter) Next(buf []byte) ([]byte, uint64)  // Get next word
func (g *Getter) Reset(offset uint64)                // Jump to offset
func (g *Getter) HasNext() bool                      // Check if more data
func (g *Getter) Skip() (uint64, int)               // Skip current word
func (g *Getter) MatchPrefix(prefix []byte) bool    // Check prefix
```

### 3. Reader Wrapper (Key-Value Pairs)

For key-value data, a Reader wrapper provides convenient iteration:

```go
rd := seg.NewReader(d.MakeGetter(), compression)
for rd.HasNext() {
    key, _ := rd.Next(nil)   // Read key
    value, _ := rd.Next(nil)  // Read value
    // Process k,v pair
}
```

## Data Access Patterns

### Pattern 1: Sequential Reading

```go
g := decompressor.MakeGetter()
g.Reset(0)  // Start from beginning
for g.HasNext() {
    data, _ := g.Next(nil)
    // Process data
}
```

### Pattern 2: Index-Based Random Access

```go
// Using recsplit index for O(1) lookup
index := segment.Index()
offset := index.OrdinalLookup(itemNumber)
g := decompressor.MakeGetter()
g.Reset(offset)
data, _ := g.Next(nil)
```

### Pattern 3: Binary Search

```go
g := seg.NewReader(decompressor.MakeGetter(), compression)
n := decompressor.Count() / 2  // For key-value pairs
offset, found := g.BinarySearch(searchKey, n, idx.OrdinalLookup)
if found {
    g.Reset(offset)
    key, _ := g.Next(nil)
    value, _ := g.Next(nil)
}
```

## Data Deserialization Examples

### Block Headers

```go
func readHeader(sn *VisibleSegment, blockHeight uint64) (*types.Header, error) {
    index := sn.Src().Index()
    offset := index.OrdinalLookup(blockHeight - index.BaseDataID())
    
    gg := sn.Src().MakeGetter()
    gg.Reset(offset)
    
    buf, _ := gg.Next(nil)
    
    // First byte is format marker, rest is RLP
    h := &types.Header{}
    if err := rlp.DecodeBytes(buf[1:], h); err != nil {
        return nil, err
    }
    return h, nil
}
```

### Transactions with Senders

```go
gg := txsSeg.Src().MakeGetter()
gg.Reset(txnOffset)

for i := uint32(0); i < txCount; i++ {
    buf, _ = gg.Next(nil)
    
    // First byte: format marker
    // Next 20 bytes: sender address
    // Rest: RLP-encoded transaction
    sender := common.Address{}
    sender.SetBytes(buf[1 : 1+20])
    
    txRlp := buf[1+20:]
    tx, err := types.DecodeTransaction(txRlp)
    tx.SetSender(sender)
}
```

### State Data (Account/Storage)

```go
g := seg.NewReader(decompressor.MakeGetter(), compression)
for g.HasNext() {
    key, _ := g.Next(nil)    // Account/storage key
    value, _ := g.Next(nil)   // Encoded state value
    
    // Decode based on domain type
    if isDomainAccount {
        var acc accounts.Account
        acc.DecodeForStorage(value)
    }
}
```

## Key Design Patterns for Rust Port

### 1. Two-Stage API

- **Low-level**: `Getter` provides word-by-word decompression
- **High-level**: `Reader` wraps Getter for convenient key-value iteration

### 2. Buffer Reuse

The `Next(buf []byte)` pattern allows buffer reuse:
```go
var buf []byte
for g.HasNext() {
    buf, _ = g.Next(buf[:0])  // Reuse buffer
}
```

### 3. Format Markers

Many seg files use the first byte as a format marker:
- `0`: No compression
- `1`: Dictionary compression
- Other values for future formats

### 4. Offset Management

- Offsets are cumulative from file start
- Indexes store offsets for random access
- Binary search uses offsets for range queries

### 5. Memory Efficiency

- Dictionary loaded once on file open
- Streaming decompression (no full file load)
- Buffer reuse for minimal allocations

## Rust Implementation Recommendations

### 1. Core Types

```rust
pub struct Decompressor {
    dict: Dictionary,
    data: Vec<u8>,  // Or mmap
    word_count: u64,
}

pub struct Getter<'a> {
    decompressor: &'a Decompressor,
    position: usize,
    word_idx: u64,
}

pub struct Reader<'a> {
    getter: Getter<'a>,
    next_is_value: bool,
}
```

### 2. Trait Design

```rust
trait SegmentReader {
    fn next(&mut self, buf: Vec<u8>) -> Option<(Vec<u8>, u64)>;
    fn reset(&mut self, offset: u64);
    fn has_next(&self) -> bool;
}
```

### 3. Index Integration

```rust
trait IndexLookup {
    fn ordinal_lookup(&self, n: u64) -> u64;
    fn binary_search(&self, key: &[u8]) -> Option<u64>;
}
```

### 4. Data Processing

```rust
// Example: Reading blockchain data
fn read_header(seg: &Decompressor, index: &Index, height: u64) -> Result<Header> {
    let offset = index.ordinal_lookup(height);
    let mut getter = seg.make_getter();
    getter.reset(offset);
    
    let data = getter.next(Vec::new())?;
    // Skip format byte, decode RLP
    rlp::decode(&data[1..])
}
```

## Testing Strategy

1. **Round-trip tests**: Compress → Decompress → Verify
2. **Cross-compatibility**: Ensure Rust can read Go-compressed files
3. **Performance benchmarks**: Compare with Go implementation
4. **Fuzz testing**: Random data compression/decompression
5. **Real data tests**: Use actual Erigon snapshot files

## File Types to Support

Priority order for implementation:

1. **Basic seg files**: Raw compressed data
2. **Key-value files**: Domain, history, index files
3. **Block data**: Headers, bodies, transactions
4. **Index files**: Recsplit indexes for random access

## Performance Considerations

- **Memory-mapped files**: Consider mmap for large files
- **Parallel decompression**: Multiple getters for concurrent access
- **Buffer pooling**: Reuse buffers across operations
- **Zero-copy where possible**: Avoid unnecessary allocations