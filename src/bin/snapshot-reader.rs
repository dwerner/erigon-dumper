use alloy_consensus::Header;
use alloy_rlp::Decodable;
use erigon_dumper::decompress::Decompressor;
use erigon_dumper::snapshots::recsplit::RecSplitIndex;
use std::path::Path;

struct SnapshotReader {
    headers_seg: Decompressor,
    headers_idx: RecSplitIndex,
    bodies_seg: Decompressor,
    bodies_idx: RecSplitIndex,
    transactions_seg: Decompressor,
    transactions_idx: RecSplitIndex,
    tx_to_block_idx: RecSplitIndex,
    base_block: u64,
}

impl SnapshotReader {
    fn new(
        snapshot_dir: &Path,
        range_start: u64,
        range_end: u64,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let range_str = format!("{:06}-{:06}", range_start / 1000, range_end / 1000);

        let headers_seg =
            Decompressor::new(snapshot_dir.join(format!("v1-{}-headers.seg", range_str)))?;
        let headers_idx =
            RecSplitIndex::open(&snapshot_dir.join(format!("v1-{}-headers.idx", range_str)))?;

        let bodies_seg =
            Decompressor::new(snapshot_dir.join(format!("v1-{}-bodies.seg", range_str)))?;
        let bodies_idx =
            RecSplitIndex::open(&snapshot_dir.join(format!("v1-{}-bodies.idx", range_str)))?;

        let transactions_seg =
            Decompressor::new(snapshot_dir.join(format!("v1-{}-transactions.seg", range_str)))?;
        let transactions_idx =
            RecSplitIndex::open(&snapshot_dir.join(format!("v1-{}-transactions.idx", range_str)))?;

        let tx_to_block_idx = RecSplitIndex::open(
            &snapshot_dir.join(format!("v1-{}-transactions-to-block.idx", range_str)),
        )?;

        Ok(SnapshotReader {
            headers_seg,
            headers_idx,
            bodies_seg,
            bodies_idx,
            transactions_seg,
            transactions_idx,
            tx_to_block_idx,
            base_block: range_start,
        })
    }

    fn read_header(&self, block_number: u64) -> Option<Header> {
        let ordinal = block_number - self.base_block;
        let offset = self.headers_idx.ordinal_lookup(ordinal)?;

        let mut getter = self.headers_seg.make_getter();
        getter.reset(offset);

        if !getter.has_next() {
            return None;
        }

        let (word, _) = getter.next(Vec::new());
        if word.is_empty() || word.len() < 2 {
            return None;
        }

        // Skip first byte (hash[0]) and decode header RLP
        Header::decode(&mut &word[1..]).ok()
    }

    fn read_body(&self, block_number: u64) -> Option<Vec<u8>> {
        let ordinal = block_number - self.base_block;
        let offset = self.bodies_idx.ordinal_lookup(ordinal)?;

        let mut getter = self.bodies_seg.make_getter();
        getter.reset(offset);

        if !getter.has_next() {
            return None;
        }

        let (word, _) = getter.next(Vec::new());
        if word.is_empty() {
            None
        } else {
            Some(word)
        }
    }

    fn read_transactions(&self, start_tx_id: u64, count: u64) -> Vec<Vec<u8>> {
        let mut transactions = Vec::new();

        for i in 0..count {
            let tx_ordinal = start_tx_id + i;
            if let Some(offset) = self.transactions_idx.ordinal_lookup(tx_ordinal) {
                let mut getter = self.transactions_seg.make_getter();
                getter.reset(offset);

                if getter.has_next() {
                    let (word, _) = getter.next(Vec::new());
                    if !word.is_empty() {
                        transactions.push(word);
                    }
                }
            }
        }

        transactions
    }

    fn get_stats(&self) -> SnapshotStats {
        SnapshotStats {
            headers_count: self.headers_idx.key_count(),
            bodies_count: self.bodies_idx.key_count(),
            transactions_count: self.transactions_idx.key_count(),
            headers_data_size: self.headers_seg.size(),
            bodies_data_size: self.bodies_seg.size(),
            transactions_data_size: self.transactions_seg.size(),
            base_block: self.base_block,
        }
    }
}

struct SnapshotStats {
    headers_count: u64,
    bodies_count: u64,
    transactions_count: u64,
    headers_data_size: usize,
    bodies_data_size: usize,
    transactions_data_size: usize,
    base_block: u64,
}

impl std::fmt::Display for SnapshotStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "=== Snapshot Statistics ===")?;
        writeln!(f, "Base block: {}", self.base_block)?;
        writeln!(
            f,
            "Headers: {} entries, {} bytes",
            self.headers_count, self.headers_data_size
        )?;
        writeln!(
            f,
            "Bodies: {} entries, {} bytes",
            self.bodies_count, self.bodies_data_size
        )?;
        writeln!(
            f,
            "Transactions: {} entries, {} bytes",
            self.transactions_count, self.transactions_data_size
        )?;
        writeln!(
            f,
            "Total data: {} bytes",
            self.headers_data_size + self.bodies_data_size + self.transactions_data_size
        )?;
        Ok(())
    }
}

fn analyze_block_completeness(reader: &SnapshotReader, start_block: u64, count: u64) {
    println!("=== Block Completeness Analysis ===");

    let mut complete_blocks = 0;
    let mut missing_headers = 0;
    let mut missing_bodies = 0;

    for i in 0..count {
        let block_num = start_block + i;
        let has_header = reader.read_header(block_num).is_some();
        let has_body = reader.read_body(block_num).is_some();

        match (has_header, has_body) {
            (true, true) => complete_blocks += 1,
            (false, true) => missing_headers += 1,
            (true, false) => missing_bodies += 1,
            (false, false) => {
                // Both missing - don't count separately
            }
        }
    }

    println!("Blocks analyzed: {}", count);
    println!("Complete blocks (header + body): {}", complete_blocks);
    println!("Missing headers only: {}", missing_headers);
    println!("Missing bodies only: {}", missing_bodies);
    println!(
        "Completeness rate: {:.1}%",
        (complete_blocks as f64 / count as f64) * 100.0
    );
}

fn sample_block_data(reader: &SnapshotReader, block_number: u64) {
    println!("\n=== Sample Block Data: {} ===", block_number);

    if let Some(header) = reader.read_header(block_number) {
        println!("Header:");
        println!("  Block number: {}", header.number);
        println!("  Timestamp: {}", header.timestamp);
        println!("  Hash: {:?}", header.hash_slow());
        println!("  Parent hash: {:?}", header.parent_hash);
        println!("  Gas used: {}", header.gas_used);
        println!("  Gas limit: {}", header.gas_limit);
    } else {
        println!("Header: Missing or empty");
    }

    if let Some(body) = reader.read_body(block_number) {
        println!("Body:");
        println!("  Size: {} bytes", body.len());
        println!("  First 20 bytes: {:02x?}", &body[..body.len().min(20)]);

        // Try to decode the body RLP to count transactions
        use alloy_consensus::TxEnvelope;
        use alloy_rlp::Decodable;
        if let Ok(body_decoded) = alloy_consensus::BlockBody::<TxEnvelope>::decode(&mut &body[..]) {
            println!("  Transactions: {}", body_decoded.transactions.len());
            println!(
                "  Withdrawals: {}",
                body_decoded.withdrawals.as_ref().map_or(0, |w| w.len())
            );
        } else {
            println!("  Could not decode body RLP");
        }
    } else {
        println!("Body: Missing or empty");
    }
}

fn main() {
    let snapshot_dir = Path::new("test_data/snapshots");

    println!("Loading snapshots from: {:?}", snapshot_dir);

    let reader = match SnapshotReader::new(snapshot_dir, 23070000, 23071000) {
        Ok(reader) => reader,
        Err(e) => {
            eprintln!("Failed to load snapshots: {}", e);
            return;
        }
    };

    // Show statistics
    println!("{}", reader.get_stats());

    // Analyze completeness
    analyze_block_completeness(&reader, 23070000, 1000);

    // Sample some blocks
    sample_block_data(&reader, 23070000); // First block
    sample_block_data(&reader, 23070054); // Block we know has header
    sample_block_data(&reader, 23070055); // Block we know has empty header
    sample_block_data(&reader, 23070999); // Last block

    println!("\n=== Transaction Analysis ===");
    let tx_stats = reader.get_stats();
    println!(
        "Total transactions in snapshot: {}",
        tx_stats.transactions_count
    );

    // Sample some transactions
    println!("\nFirst 3 transactions:");
    let first_txs = reader.read_transactions(0, 3);
    for (i, tx) in first_txs.iter().enumerate() {
        println!(
            "  TX {}: {} bytes, first 10 bytes: {:02x?}",
            i,
            tx.len(),
            &tx[..tx.len().min(10)]
        );
    }
}
