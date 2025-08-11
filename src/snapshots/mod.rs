pub mod error;
pub mod index;
pub mod reader;

pub use error::{Result, SnapshotError};
pub use index::IndexReader;
pub use reader::HeadersReader;

#[cfg(test)]
mod tests {
    use alloy_consensus::Header;
    use alloy_primitives::{FixedBytes, B256};
    use alloy_rlp::{Decodable, Encodable};

    #[test]
    fn test_alloy_header_type() {
        // Verify that alloy-consensus Header type works for our needs
        let header = Header {
            parent_hash: B256::ZERO,
            ommers_hash: B256::ZERO,
            beneficiary: Default::default(),
            state_root: B256::ZERO,
            transactions_root: B256::ZERO,
            receipts_root: B256::ZERO,
            logs_bloom: Default::default(),
            difficulty: Default::default(),
            number: 100,
            gas_limit: 8_000_000,
            gas_used: 5_000_000,
            timestamp: 1_600_000_000,
            extra_data: Default::default(),
            mix_hash: B256::ZERO,
            nonce: FixedBytes::default(),
            base_fee_per_gas: Some(1_000_000_000),
            withdrawals_root: None,
            blob_gas_used: None,
            excess_blob_gas: None,
            parent_beacon_block_root: None,
            requests_hash: None,
            target_blobs_per_block: None,
        };

        assert_eq!(header.number, 100);

        // Test encoding/decoding
        let encoded = alloy_rlp::encode(&header);
        assert!(!encoded.is_empty());

        let decoded = Header::decode(&mut encoded.as_ref()).unwrap();
        assert_eq!(decoded.number, header.number);
        assert_eq!(decoded.gas_limit, header.gas_limit);
        assert_eq!(decoded.timestamp, header.timestamp);

        // Test hash calculation
        let hash = header.hash_slow();
        assert_ne!(hash, B256::ZERO);
    }

    #[test]
    fn test_header_with_eip1559_fields() {
        // Test London fork headers with base_fee_per_gas
        let header = Header {
            parent_hash: B256::ZERO,
            ommers_hash: B256::ZERO,
            beneficiary: Default::default(),
            state_root: B256::ZERO,
            transactions_root: B256::ZERO,
            receipts_root: B256::ZERO,
            logs_bloom: Default::default(),
            difficulty: Default::default(),
            number: 12_965_000, // London fork block
            gas_limit: 30_000_000,
            gas_used: 20_000_000,
            timestamp: 1_628_166_812,
            extra_data: Default::default(),
            mix_hash: B256::ZERO,
            nonce: FixedBytes::default(),
            base_fee_per_gas: Some(1_000_000_000), // 1 gwei
            withdrawals_root: None,
            blob_gas_used: None,
            excess_blob_gas: None,
            parent_beacon_block_root: None,
            requests_hash: None,
            target_blobs_per_block: None,
        };

        // Encode and decode
        let encoded = alloy_rlp::encode(&header);
        let decoded = Header::decode(&mut encoded.as_ref()).unwrap();

        assert_eq!(decoded.base_fee_per_gas, Some(1_000_000_000));
    }

    #[test]
    fn test_header_with_eip4844_fields() {
        // Test Cancun fork headers with blob gas fields
        let header = Header {
            parent_hash: B256::ZERO,
            ommers_hash: B256::ZERO,
            beneficiary: Default::default(),
            state_root: B256::ZERO,
            transactions_root: B256::ZERO,
            receipts_root: B256::ZERO,
            logs_bloom: Default::default(),
            difficulty: Default::default(),
            number: 19_426_587, // Cancun fork block
            gas_limit: 30_000_000,
            gas_used: 15_000_000,
            timestamp: 1_710_338_135,
            extra_data: Default::default(),
            mix_hash: B256::ZERO,
            nonce: FixedBytes::default(),
            base_fee_per_gas: Some(35_000_000_000),
            withdrawals_root: Some(B256::ZERO),
            blob_gas_used: Some(131_072), // 1 blob
            excess_blob_gas: Some(0),
            parent_beacon_block_root: Some(B256::ZERO),
            requests_hash: None,
            target_blobs_per_block: None,
        };

        let encoded = alloy_rlp::encode(&header);
        let decoded = Header::decode(&mut encoded.as_ref()).unwrap();

        assert_eq!(decoded.blob_gas_used, Some(131_072));
        assert_eq!(decoded.excess_blob_gas, Some(0));
        assert!(decoded.parent_beacon_block_root.is_some());
    }
}
