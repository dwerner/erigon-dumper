use erigon_dumper::snapshots::HeadersReader;
use std::path::Path;

fn main() {
    let path = Path::new("test_data/snapshots/v1-023070-023071-headers.seg");
    let reader = HeadersReader::new(path).expect("Failed to open snapshot");
    
    println!("Total headers: {}", reader.count());
    
    let mut getter = reader.make_getter();
    
    // Try to read first header
    if getter.has_next() {
        let (hash, header) = getter.next().expect("Failed to read first header");
        println!("First header: Block #{}, hash: 0x{}", header.number, hex::encode(&hash[..8]));
    }
    
    // Now try to skip one
    println!("Skipping one header...");
    if getter.has_next() {
        getter.skip();
        println!("Skip completed");
    }
    
    // Try to read after skip
    if getter.has_next() {
        let (hash, header) = getter.next().expect("Failed to read after skip");
        println!("After skip: Block #{}, hash: 0x{}", header.number, hex::encode(&hash[..8]));
    } else {
        println!("No more headers after skip");
    }
    
    // Try multiple skips
    println!("\nTrying multiple skips...");
    for i in 0..10 {
        if getter.has_next() {
            getter.skip();
            println!("Skip {} completed", i + 1);
        } else {
            println!("No more headers at skip {}", i + 1);
            break;
        }
    }
    
    // Try to read after multiple skips
    if getter.has_next() {
        match getter.next() {
            Ok((hash, header)) => {
                println!("After 10 skips: Block #{}, hash: 0x{}", header.number, hex::encode(&hash[..8]));
            }
            Err(e) => {
                println!("Error reading after skips: {}", e);
            }
        }
    } else {
        println!("No more headers after skips");
    }
}