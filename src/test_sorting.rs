// Test to verify sorting behavior matches Go
use crate::compress_go_port2::Position;

#[test]
fn test_position_sorting() {
    // Create positions with uses=0 (as they are after SetDepth)
    let mut positions = vec![
        Position { pos: 0, uses: 0, code: 0, code_bits: 1, depth: 1 },
        Position { pos: 17, uses: 0, code: 1, code_bits: 4, depth: 4 },
        Position { pos: 19, uses: 0, code: 9, code_bits: 4, depth: 4 },
        Position { pos: 5, uses: 0, code: 5, code_bits: 3, depth: 3 },
        Position { pos: 1, uses: 0, code: 3, code_bits: 2, depth: 2 },
    ];
    
    println!("Before sorting:");
    for p in &positions {
        println!("  pos={}, code={}, reverse={:#018x}", p.pos, p.code, p.code.reverse_bits());
    }
    
    // Sort using the same logic as in compress_go_port2
    positions.sort_by(|a, b| {
        if a.uses == b.uses {
            b.code.reverse_bits().cmp(&a.code.reverse_bits())
        } else {
            a.uses.cmp(&b.uses)
        }
    });
    
    println!("\nAfter sorting:");
    for p in &positions {
        println!("  pos={}, code={}, reverse={:#018x}", p.pos, p.code, p.code.reverse_bits());
    }
    
    // Expected order based on reverse bits (descending):
    // code=3 -> reverse=0xc000000000000000
    // code=5 -> reverse=0xa000000000000000  
    // code=9 -> reverse=0x9000000000000000
    // code=1 -> reverse=0x8000000000000000
    // code=0 -> reverse=0x0000000000000000
    
    assert_eq!(positions[0].code, 3, "First should be code=3");
    assert_eq!(positions[1].code, 5, "Second should be code=5");
    assert_eq!(positions[2].code, 9, "Third should be code=9");
    assert_eq!(positions[3].code, 1, "Fourth should be code=1");
    assert_eq!(positions[4].code, 0, "Fifth should be code=0");
}