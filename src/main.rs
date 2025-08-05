use erigon_dumper::ErigonReader;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Try to load the extracted snapshot files
    let test_data_path = Path::new("test_data");
    
    println!("Attempting to load Erigon data from: {:?}", test_data_path);
    
    match ErigonReader::open(test_data_path) {
        Ok(reader) => {
            println!("✅ Successfully opened Erigon reader");
            
            // First, let's see what segments are loaded
            reader.debug_segments();
            
            // Test a smaller range first to see if we can find anything
            let mut found_any = false;
            for block_num in 23_070_000..23_070_010 {
                if reader.has_block(block_num) {
                    println!("✅ Found block {}", block_num);
                    found_any = true;
                    
                    match reader.read_header(block_num) {
                        Ok(header) => {
                            println!("  ✅ Successfully read header!");
                            println!("  Header hash: 0x{}", hex::encode(header.hash_slow()));
                            println!("  Block number: {}", header.number);
                            println!("  Parent hash: 0x{}", hex::encode(header.parent_hash));
                            break; // Stop after first successful read
                        }
                        Err(e) => {
                            println!("  ❌ Failed to read header: {}", e);
                        }
                    }
                }
            }
            
            if !found_any {
                println!("❌ No blocks found in range 23,070,000-23,070,010");
                println!("Let me check what block ranges are actually available...");
            }
        }
        Err(e) => {
            println!("❌ Failed to open reader: {}", e);
        }
    }
    
    Ok(())
}
