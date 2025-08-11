use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    let args: Vec<String> = env::args().collect();

    let search_dirs = if args.len() > 1 {
        args[1..].iter().map(PathBuf::from).collect()
    } else {
        // Common Erigon snapshot locations
        vec![
            PathBuf::from("./snapshots"),
            PathBuf::from("./chaindata/snapshots"),
            PathBuf::from(env::var("HOME").unwrap_or_default())
                .join(".local/share/erigon/snapshots"),
            PathBuf::from(env::var("HOME").unwrap_or_default()).join("Library/Erigon/snapshots"),
        ]
    };

    println!("Searching for Erigon snapshot files...\n");

    for dir in search_dirs {
        if !dir.exists() {
            continue;
        }

        println!("Checking: {:?}", dir);

        match fs::read_dir(&dir) {
            Ok(entries) => {
                let mut snapshots = Vec::new();

                for entry in entries.flatten() {
                    let path = entry.path();
                    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                        if name.ends_with(".seg") {
                            snapshots.push(name.to_string());
                        }
                    }
                }

                if !snapshots.is_empty() {
                    snapshots.sort();
                    println!("  Found {} snapshot files:", snapshots.len());

                    // Group by type
                    let headers: Vec<_> =
                        snapshots.iter().filter(|s| s.contains("headers")).collect();
                    let bodies: Vec<_> =
                        snapshots.iter().filter(|s| s.contains("bodies")).collect();
                    let transactions: Vec<_> = snapshots
                        .iter()
                        .filter(|s| s.contains("transactions"))
                        .collect();

                    if !headers.is_empty() {
                        println!("\n  Headers ({}):", headers.len());
                        for (i, h) in headers.iter().take(5).enumerate() {
                            println!("    {}", h);
                            if i == 4 && headers.len() > 5 {
                                println!("    ... and {} more", headers.len() - 5);
                            }
                        }
                    }

                    if !bodies.is_empty() {
                        println!("\n  Bodies ({}):", bodies.len());
                        for (i, b) in bodies.iter().take(5).enumerate() {
                            println!("    {}", b);
                            if i == 4 && bodies.len() > 5 {
                                println!("    ... and {} more", bodies.len() - 5);
                            }
                        }
                    }

                    if !transactions.is_empty() {
                        println!("\n  Transactions ({}):", transactions.len());
                        for (i, t) in transactions.iter().take(5).enumerate() {
                            println!("    {}", t);
                            if i == 4 && transactions.len() > 5 {
                                println!("    ... and {} more", transactions.len() - 5);
                            }
                        }
                    }

                    println!("\nExample usage:");
                    if let Some(first_header) = headers.first() {
                        println!(
                            "  cargo run --example read_snapshot {}",
                            dir.join(first_header).display()
                        );
                    }
                }
            }
            Err(e) => {
                println!("  Error reading directory: {}", e);
            }
        }
        println!();
    }

    println!("\nTo search in a specific directory:");
    println!("  cargo run --example find_snapshots /path/to/erigon/snapshots");
}
