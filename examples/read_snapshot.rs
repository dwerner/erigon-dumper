use erigon_dumper::snapshots::HeadersReader;
use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <path-to-headers.seg>", args[0]);
        eprintln!(
            "Example: {} /path/to/erigon/snapshots/v1-000000-000500-headers.seg",
            args[0]
        );
        std::process::exit(1);
    }

    let snapshot_path = PathBuf::from(&args[1]);

    if !snapshot_path.exists() {
        eprintln!("Error: File not found: {:?}", snapshot_path);
        std::process::exit(1);
    }

    println!("Opening snapshot: {:?}", snapshot_path);
    let reader = HeadersReader::new(&snapshot_path)?;

    println!("Total headers in snapshot: {}", reader.count());

    let mut getter = reader.make_getter();
    let mut count = 0;
    let max_display = 10;

    println!("\nFirst {} headers:", max_display);
    println!("{:-<80}", "");

    while getter.has_next() && count < max_display {
        let (hash, header) = getter.next()?;

        println!("Block #{}", header.number);
        println!("  Hash:       0x{}", hex::encode(&hash[..]));
        println!("  Parent:     0x{}", hex::encode(&header.parent_hash[..]));
        println!(
            "  Timestamp:  {} ({})",
            header.timestamp,
            chrono::DateTime::from_timestamp(header.timestamp as i64, 0)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                .unwrap_or_else(|| "invalid".to_string())
        );
        println!("  Gas limit:  {}", header.gas_limit);
        println!("  Gas used:   {}", header.gas_used);

        if let Some(base_fee) = header.base_fee_per_gas {
            println!("  Base fee:   {} gwei", base_fee / 1_000_000_000);
        }

        println!();
        count += 1;
    }

    // Show some statistics
    if reader.count() > max_display {
        println!("... {} more headers in file", reader.count() - max_display);
    }

    Ok(())
}
