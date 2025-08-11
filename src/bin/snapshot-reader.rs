use clap::{Parser, Subcommand};
use erigon_dumper::snapshots::{HeadersReader, IndexReader, Result, SnapshotError};
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(name = "snapshot-reader")]
#[command(about = "Read and inspect Erigon snapshot files", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Enable debug logging
    #[arg(short, long)]
    debug: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Read headers from a snapshot file
    Headers {
        /// Path to the headers .seg file
        #[arg(value_name = "FILE")]
        snapshot: PathBuf,

        /// Optional path to the .idx index file for fast lookups
        #[arg(short, long)]
        index: Option<PathBuf>,

        /// Block number to look up (requires index)
        #[arg(short, long)]
        block: Option<u64>,

        /// Number of headers to display (default: 10)
        #[arg(short, long, default_value = "10")]
        count: usize,

        /// Skip this many headers before displaying
        #[arg(short, long, default_value = "0")]
        skip: usize,
    },

    /// Read bodies from a snapshot file
    Bodies {
        /// Path to the bodies .seg file
        #[arg(value_name = "FILE")]
        snapshot: PathBuf,

        /// Optional path to the .idx index file
        #[arg(short, long)]
        index: Option<PathBuf>,
    },

    /// Read transactions from a snapshot file
    Transactions {
        /// Path to the transactions .seg file
        #[arg(value_name = "FILE")]
        snapshot: PathBuf,

        /// Optional path to the .idx index file
        #[arg(short, long)]
        index: Option<PathBuf>,
    },

    /// Show information about a snapshot file
    Info {
        /// Path to the .seg file
        #[arg(value_name = "FILE")]
        snapshot: PathBuf,

        /// Optional path to the .idx index file
        #[arg(short, long)]
        index: Option<PathBuf>,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Set up logging
    let log_level = if cli.debug { "debug" } else { "info" };
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(log_level)).init();

    match cli.command {
        Commands::Headers {
            snapshot,
            index,
            block,
            count,
            skip,
        } => read_headers(&snapshot, index.as_deref(), block, count, skip),
        Commands::Bodies { snapshot, index } => {
            println!("Bodies reading not yet implemented");
            println!("Snapshot: {:?}", snapshot);
            if let Some(idx) = index {
                println!("Index: {:?}", idx);
            }
            Ok(())
        }
        Commands::Transactions { snapshot, index } => {
            println!("Transactions reading not yet implemented");
            println!("Snapshot: {:?}", snapshot);
            if let Some(idx) = index {
                println!("Index: {:?}", idx);
            }
            Ok(())
        }
        Commands::Info { snapshot, index } => show_info(&snapshot, index.as_deref()),
    }
}

fn read_headers(
    snapshot_path: &PathBuf,
    index_path: Option<&Path>,
    block_num: Option<u64>,
    count: usize,
    skip: usize,
) -> Result<()> {
    if !snapshot_path.exists() {
        return Err(SnapshotError::InvalidPath(format!(
            "Snapshot file not found: {:?}",
            snapshot_path
        )));
    }

    let reader = HeadersReader::new(snapshot_path)?;
    println!("Opened snapshot with {} headers", reader.count());

    // If block number specified, we need an index
    if let Some(block) = block_num {
        let idx_path = index_path.ok_or(SnapshotError::IndexNotAvailable)?;
        if !idx_path.exists() {
            return Err(SnapshotError::InvalidPath(format!(
                "Index file not found: {:?}",
                idx_path
            )));
        }

        let index = IndexReader::new(idx_path)?;
        println!("Loaded index with {} entries", index.key_count());

        // Look up the block
        let offset = index
            .lookup(block)
            .ok_or(SnapshotError::BlockNotFound(block))?;

        println!("Block {} found at offset {}", block, offset);

        // Read the header at that offset
        let mut getter = reader.make_getter();
        getter.reset(offset);

        if getter.has_next() {
            let (hash, header) = getter.next()?;
            print_header(block, &hash, &header);
        }
    } else {
        // Sequential reading
        let mut getter = reader.make_getter();

        // Skip requested number of headers
        for _ in 0..skip {
            if !getter.has_next() {
                break;
            }
            getter.skip();
        }

        // Read and display headers
        let mut displayed = 0;
        let mut _current_block = skip as u64; // Assuming sequential from 0

        while getter.has_next() && displayed < count {
            let (hash, header) = getter.next()?;
            print_header(header.number, &hash, &header);
            displayed += 1;
            _current_block += 1;
        }

        if reader.count() > skip + count {
            println!(
                "\n... {} more headers in file",
                reader.count() - skip - count
            );
        }
    }

    Ok(())
}

fn print_header(block_num: u64, hash: &alloy_primitives::B256, header: &alloy_consensus::Header) {
    println!("\nBlock #{}", block_num);
    println!("  Hash:       0x{}", hex::encode(&hash[..]));
    println!("  Parent:     0x{}", hex::encode(&header.parent_hash[..]));

    #[cfg(feature = "chrono")]
    {
        use chrono::DateTime;
        let timestamp_str = DateTime::from_timestamp(header.timestamp as i64, 0)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
            .unwrap_or_else(|| "invalid".to_string());
        println!("  Timestamp:  {} ({})", header.timestamp, timestamp_str);
    }
    #[cfg(not(feature = "chrono"))]
    {
        println!("  Timestamp:  {}", header.timestamp);
    }

    println!("  Gas limit:  {}", header.gas_limit);
    println!("  Gas used:   {}", header.gas_used);
    println!("  Difficulty: {}", header.difficulty);

    if let Some(base_fee) = header.base_fee_per_gas {
        println!("  Base fee:   {} gwei", base_fee / 1_000_000_000);
    }

    if let Some(blob_gas) = header.blob_gas_used {
        println!("  Blob gas:   {}", blob_gas);
    }
}

fn show_info(snapshot_path: &PathBuf, index_path: Option<&Path>) -> Result<()> {
    if !snapshot_path.exists() {
        return Err(SnapshotError::InvalidPath(format!(
            "Snapshot file not found: {:?}",
            snapshot_path
        )));
    }

    // Get file info
    let metadata = std::fs::metadata(snapshot_path)?;
    let file_size = metadata.len();

    println!("Snapshot file: {:?}", snapshot_path);
    println!(
        "File size: {} bytes ({:.2} MB)",
        file_size,
        file_size as f64 / 1_048_576.0
    );

    // Determine snapshot type from filename
    let filename = snapshot_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown");

    let snapshot_type = if filename.contains("headers") {
        "Headers"
    } else if filename.contains("bodies") {
        "Bodies"
    } else if filename.contains("transactions") {
        "Transactions"
    } else {
        "Unknown"
    };

    println!("Type: {}", snapshot_type);

    // Try to parse block range from filename (e.g., v1-000000-000500-headers.seg)
    let captures = filename.split('-').collect::<Vec<_>>();
    if captures.len() >= 4 {
        if let (Ok(from), Ok(to)) = (captures[1].parse::<u64>(), captures[2].parse::<u64>()) {
            println!("Block range: {} - {} ({} blocks)", from, to, to - from);
        }
    }

    // Open and get info from decompressor
    match snapshot_type {
        "Headers" => {
            let reader = HeadersReader::new(snapshot_path)?;
            println!("Entries: {} headers", reader.count());
        }
        _ => {
            println!("Entries: (reader not implemented for this type)");
        }
    }

    // Check index file if provided
    if let Some(idx_path) = index_path {
        if !idx_path.exists() {
            println!("\nIndex file not found: {:?}", idx_path);
        } else {
            let idx_metadata = std::fs::metadata(idx_path)?;
            let idx_size = idx_metadata.len();

            println!("\nIndex file: {:?}", idx_path);
            println!(
                "Index size: {} bytes ({:.2} KB)",
                idx_size,
                idx_size as f64 / 1024.0
            );

            let index = IndexReader::new(idx_path)?;
            println!("Index entries: {}", index.key_count());
            println!(
                "Index type: {}",
                if index.is_enum() {
                    "Enum (sequential)"
                } else {
                    "Hash"
                }
            );
        }
    }

    Ok(())
}
