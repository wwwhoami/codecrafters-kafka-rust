use std::io::{BufReader, Read};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::Result;

use super::{
    bytes::{FromBytes, ToBytes},
    error::{self, IoError},
};

#[derive(Debug)]
pub(crate) enum ApiKey {
    ApiVersions = 18,
    DescribeTopicPartitions = 75,
}

impl ToBytes for ApiKey {
    fn to_be_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(2);
        let val = match self {
            ApiKey::ApiVersions => 18_i16,
            ApiKey::DescribeTopicPartitions => 75_i16,
        };

        buf.put_i16(val);
        buf.freeze()
    }
}

impl FromBytes for ApiKey {
    fn from_be_bytes<B: Buf>(buf: &mut B) -> Result<Self> {
        let key = buf.try_get_i16()?;

        match key {
            18 => Ok(ApiKey::ApiVersions),
            75 => Ok(ApiKey::DescribeTopicPartitions),
            _ => Err(error::UnsupportedApiKeyError::new(key).into()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NullableString {
    value: Option<String>,
}

impl ToBytes for NullableString {
    fn to_be_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        if self.value.is_none() {
            buf.put_i16(-1);
        } else {
            let value = self.value.as_ref().unwrap();
            buf.put_i16(value.len() as i16);
            buf.put_slice(value.as_bytes());
        }

        buf.freeze()
    }
}

impl FromBytes for NullableString {
    fn from_be_bytes<B: Buf>(buf: &mut B) -> Result<Self> {
        let len = buf.try_get_i16()?;

        if len == -1 {
            Ok(NullableString { value: None })
        } else {
            let mut str_buf = vec![0u8; len as usize];
            buf.copy_to_slice(&mut str_buf);
            let value = String::from_utf8(str_buf)
                .map_err(|e| IoError::new(format!("failed to parse NullableString: {}", e)))?;

            Ok(NullableString { value: Some(value) })
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompactString {
    value: String,
}

impl CompactString {
    pub fn new(value: String) -> Self {
        Self { value }
    }

    pub fn from_str(value: &str) -> Self {
        Self {
            value: value.to_string(),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }
}

impl std::fmt::Display for CompactString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl ToBytes for CompactString {
    fn to_be_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        // Adjust the length to match the protocol
        let len = UnsignedVarInt::new((self.value.len() + 1) as u32);

        buf.put_slice(len.to_be_bytes().as_ref());
        if len.value > 0 {
            buf.put_slice(self.value.as_bytes());
        }

        buf.freeze()
    }
}

impl FromBytes for CompactString {
    fn from_be_bytes<B: Buf>(buf: &mut B) -> Result<Self> {
        // Adjust the length to match the protocol
        let len = UnsignedVarInt::from_be_bytes(buf)?.value - 1;

        if len == 0 {
            return Ok(CompactString {
                value: String::new(),
            });
        }

        let mut str_buf = vec![0u8; len as usize];
        buf.copy_to_slice(&mut str_buf);

        let value = String::from_utf8(str_buf)
            .map_err(|e| IoError::new(format! {"failed to parse CompactString: {}", e}))?;

        Ok(CompactString { value })
    }
}

#[derive(Debug, Clone)]
pub struct CompactArray<T> {
    array: Vec<T>,
}

impl<T> CompactArray<T> {
    pub fn from_vec(array: Vec<T>) -> Self {
        Self { array }
    }

    pub fn new() -> Self {
        Self { array: Vec::new() }
    }

    pub fn to_vec(&self) -> Vec<T>
    where
        T: Clone,
    {
        self.array.clone()
    }
}

impl<T> ToBytes for CompactArray<T>
where
    T: ToBytes,
{
    fn to_be_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        if self.array.is_empty() {
            buf.put_u8(0);
            return buf.freeze();
        }

        // Adjust length to match the protocol
        let len = UnsignedVarInt::new((self.array.len() + 1) as u32);
        buf.put_slice(len.to_be_bytes().as_ref());

        for item in &self.array {
            buf.extend_from_slice(&item.to_be_bytes());
        }

        buf.freeze()
    }
}

impl<T> FromBytes for CompactArray<T>
where
    T: FromBytes,
{
    fn from_be_bytes<B: Buf>(buf: &mut B) -> Result<Self> {
        let len = UnsignedVarInt::from_be_bytes(buf)?.value;

        match len {
            0 => Ok(CompactArray { array: Vec::new() }),
            _ => {
                let len = len - 1; // Adjust length to match the protocol
                let mut array = Vec::with_capacity(len as usize);

                for _ in 0..len {
                    array.push(T::from_be_bytes(buf)?);
                }

                Ok(CompactArray { array })
            }
        }
    }
}

// VarInt encoding/decoding follows the variable-length zig-zag encoding scheme
// from Google Protocol Buffers.
#[derive(Debug)]
pub(crate) struct VarInt {
    value: i32,
}

impl VarInt {
    pub(crate) fn value(&self) -> i32 {
        self.value
    }
}

impl From<i32> for VarInt {
    fn from(value: i32) -> Self {
        VarInt { value }
    }
}

impl FromBytes for VarInt {
    fn from_be_bytes<B: Buf>(buf: &mut B) -> Result<Self> {
        let mut result: u32 = 0;
        let mut shift = 0;

        loop {
            let byte = buf.try_get_u8().map_err(|e| {
                error::IoError::new(format!("failed to read byte for VARINT: {}", e))
            })?;

            let val = (byte & 0x7F) as u32;
            result |= val << shift;

            if (byte & 0x80) == 0 {
                // zig-zag decode
                let decoded = ((result >> 1) as i32) ^ (-((result & 1) as i32));
                return Ok(VarInt { value: decoded });
            }

            shift += 7;
            if shift > 28 {
                return Err(error::IoError::new("varint32 too long".to_string()).into());
            }
        }
    }
}

impl ToBytes for VarInt {
    fn to_be_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        // Zig-zag encode the value
        let mut value = ((self.value << 1) ^ (self.value >> 31)) as u32;

        loop {
            if (value & !0x7F) == 0 {
                buf.put_u8(value as u8);
                break;
            } else {
                buf.put_u8(((value & 0x7F) | 0x80) as u8);
                value >>= 7;
            }
        }

        buf.freeze()
    }
}

// UnsignedVarInt encoding/decoding follows the variable-length encoding scheme
// for unsigned integers, where each byte contains 7 bits of the value
// and the highest bit indicates if there are more bytes to read.
#[derive(Debug)]
pub(crate) struct UnsignedVarInt {
    value: u32,
}

impl UnsignedVarInt {
    pub(crate) fn new(value: u32) -> Self {
        Self { value }
    }

    pub(crate) fn value(&self) -> u32 {
        self.value
    }
}

impl From<u32> for UnsignedVarInt {
    fn from(value: u32) -> Self {
        UnsignedVarInt { value }
    }
}

impl FromBytes for UnsignedVarInt {
    fn from_be_bytes<B: Buf>(buf: &mut B) -> Result<Self> {
        let mut result: u32 = 0;
        let mut shift = 0;

        loop {
            let byte = buf.try_get_u8().map_err(|e| {
                error::IoError::new(format!("failed to read byte for UNSIGNED VARINT: {}", e))
            })?;

            let val = (byte & 0x7F) as u32;
            result |= val << shift;

            if (byte & 0x80) == 0 {
                return Ok(UnsignedVarInt { value: result });
            }

            shift += 7;
            if shift > 28 {
                return Err(error::IoError::new("unsigned varint32 too long".to_string()).into());
            }
        }
    }
}

impl ToBytes for UnsignedVarInt {
    fn to_be_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        let mut value = self.value;

        loop {
            if (value & !0x7F) == 0 {
                buf.put_u8(value as u8);
                break;
            } else {
                buf.put_u8(((value & 0x7F) | 0x80) as u8);
                value >>= 7;
            }
        }

        buf.freeze()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct INT16 {
    value: i16,
}

impl From<i16> for INT16 {
    fn from(value: i16) -> Self {
        INT16 { value }
    }
}

impl FromBytes for INT16 {
    fn from_be_bytes<B: Buf>(buf: &mut B) -> Result<Self> {
        let value = buf.try_get_i16()?;
        Ok(INT16 { value })
    }
}

impl ToBytes for INT16 {
    fn to_be_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(2);
        buf.put_i16(self.value);
        buf.freeze()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct INT32 {
    value: i32,
}

impl INT32 {
    pub(crate) fn value(&self) -> i32 {
        self.value
    }
}

impl From<i32> for INT32 {
    fn from(value: i32) -> Self {
        INT32 { value }
    }
}

impl std::fmt::Display for INT32 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "INT32({})", self.value)
    }
}

impl<R: Read> TryFrom<&mut BufReader<R>> for INT32 {
    type Error = std::io::Error;

    fn try_from(reader: &mut BufReader<R>) -> std::result::Result<Self, Self::Error> {
        let mut buf = [0u8; 4];

        reader.read_exact(&mut buf)?;
        let value = i32::from_be_bytes(buf);

        Ok(INT32 { value })
    }
}

impl TryFrom<bytes::Bytes> for INT32 {
    type Error = std::io::Error;

    fn try_from(mut bytes: Bytes) -> std::result::Result<Self, Self::Error> {
        let value = bytes.try_get_i32()?;
        Ok(INT32 { value })
    }
}

impl FromBytes for INT32 {
    fn from_be_bytes<B: Buf>(buf: &mut B) -> Result<Self> {
        let value = buf.try_get_i32()?;
        Ok(INT32 { value })
    }
}

impl ToBytes for INT32 {
    fn to_be_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(4);
        buf.put_i32(self.value);
        buf.freeze()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct INT64 {
    value: i64,
}

impl INT64 {
    pub(crate) fn value(&self) -> i64 {
        self.value
    }
}

impl From<i64> for INT64 {
    fn from(value: i64) -> Self {
        INT64 { value }
    }
}

impl std::fmt::Display for INT64 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "INT64({})", self.value)
    }
}

impl<R: Read> TryFrom<&mut BufReader<R>> for INT64 {
    type Error = std::io::Error;

    fn try_from(reader: &mut BufReader<R>) -> std::result::Result<Self, Self::Error> {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf)?;
        let value = i64::from_be_bytes(buf);
        Ok(INT64 { value })
    }
}

impl FromBytes for INT64 {
    fn from_be_bytes<B: Buf>(buf: &mut B) -> Result<Self> {
        let value = buf.try_get_i64()?;
        Ok(INT64 { value })
    }
}

impl ToBytes for INT64 {
    fn to_be_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(4);
        buf.put_i64(self.value);

        buf.freeze()
    }
}
