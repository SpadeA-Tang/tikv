// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp;

use bytes::{BufMut, Bytes, BytesMut};
use skiplist_rs::KeyComparator;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ValueType {
    Deletion = 0,
    Value = 1,
}

// See `compare` of InternalKeyComparator, for the same user key and same
// sequence number, ValueType::Value is less than ValueType::Deletion
pub const VALUE_TYPE_FOR_SEEK: ValueType = ValueType::Value;
pub const VALUE_TYPE_FOR_SEEK_FOR_PREV: ValueType = ValueType::Deletion;

impl TryFrom<u8> for ValueType {
    type Error = String;
    fn try_from(value: u8) -> std::prelude::v1::Result<Self, Self::Error> {
        match value {
            0 => Ok(ValueType::Deletion),
            1 => Ok(ValueType::Value),
            _ => Err(format!("invalid value: {}", value)),
        }
    }
}

pub struct InternalKey<'a> {
    pub user_key: &'a [u8],
    pub v_type: ValueType,
    pub sequence: u64,
}

const ENC_KEY_SEQ_LENGTH: usize = std::mem::size_of::<u64>();

impl<'a> From<&'a [u8]> for InternalKey<'a> {
    fn from(encoded_key: &'a [u8]) -> Self {
        decode_key(encoded_key)
    }
}

#[inline]
pub fn decode_key(encoded_key: &[u8]) -> InternalKey<'_> {
    assert!(encoded_key.len() >= ENC_KEY_SEQ_LENGTH);
    let seq_offset = encoded_key.len() - ENC_KEY_SEQ_LENGTH;
    let num = u64::from_be_bytes(
        encoded_key[seq_offset..seq_offset + ENC_KEY_SEQ_LENGTH]
            .try_into()
            .unwrap(),
    );
    let sequence = num >> 8;
    let v_type = ((num & 0xff) as u8).try_into().unwrap();
    InternalKey {
        user_key: &encoded_key[..seq_offset],
        v_type,
        sequence,
    }
}

/// Format for an internal key (used by the skip list.)
/// ```
/// contents:      key of size n     | value type | sequence number shifted by 8 bits
/// byte position:         0 ..  n-1 | n          |  n + 1 .. n + 7
/// ```
/// value type 0 encodes deletion, value type 1 encodes value.
#[inline]
pub fn encode_key_internal<T: BufMut>(
    key: &[u8],
    mvcc: Option<u64>,
    seq: u64,
    v_type: ValueType,
    f: impl FnOnce(usize) -> T,
) -> T {
    assert!(seq == u64::MAX || seq >> ((ENC_KEY_SEQ_LENGTH - 1) * 8) == 0);
    let mvcc_len = if mvcc.is_some() {
        ENC_KEY_SEQ_LENGTH
    } else {
        0
    };
    let mut e = f(key.len() + ENC_KEY_SEQ_LENGTH + mvcc_len);
    e.put(key);
    if let Some(mvcc) = mvcc {
        e.put_u64(!mvcc);
    }
    e.put_u64((seq << 8) | v_type as u64);
    e
}

#[inline]
pub fn encode_key(key: &[u8], seq: u64, v_type: ValueType) -> Bytes {
    let e = encode_key_internal::<BytesMut>(key, None, seq, v_type, BytesMut::with_capacity);
    e.freeze()
}

#[inline]
pub fn encode_seek_key(key: &[u8], seq: u64, v_type: ValueType) -> Vec<u8> {
    encode_key_internal::<Vec<_>>(key, None, seq, v_type, Vec::with_capacity)
}

#[inline]
pub fn encode_seek_key_for_bound(key: &[u8], mvcc: u64, seq: u64, v_type: ValueType) -> Vec<u8> {
    encode_key_internal::<Vec<_>>(key, Some(mvcc), seq, v_type, Vec::with_capacity)
}

#[derive(Default, Debug, Clone, Copy)]
pub struct InternalKeyComparator {}

impl InternalKeyComparator {
    fn same_key(lhs: &[u8], rhs: &[u8]) -> bool {
        let k_1 = decode_key(lhs);
        let k_2 = decode_key(rhs);
        k_1.user_key == k_2.user_key
    }
}

impl KeyComparator for InternalKeyComparator {
    fn compare_key(&self, lhs: &[u8], rhs: &[u8]) -> cmp::Ordering {
        let k_1 = decode_key(lhs);
        let k_2 = decode_key(rhs);
        let r = k_1.user_key.cmp(k_2.user_key);
        if r.is_eq() {
            match k_1.sequence.cmp(&k_2.sequence) {
                cmp::Ordering::Greater => {
                    return cmp::Ordering::Less;
                }
                cmp::Ordering::Less => {
                    return cmp::Ordering::Greater;
                }
                cmp::Ordering::Equal => {
                    return cmp::Ordering::Equal;
                }
            }
        }
        r
    }

    fn same_key(&self, lhs: &[u8], rhs: &[u8]) -> bool {
        InternalKeyComparator::same_key(lhs, rhs)
    }
}
