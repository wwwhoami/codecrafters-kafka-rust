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

impl ToBytes for CompactString {
    fn to_be_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        if self.value.is_empty() {
            buf.put_u8(0);
            return buf.freeze();
        }
        let len = self.value.len() as u8;
        buf.put_u8(len + 1); // +1 to match the protocol
        buf.put_slice(self.value.as_bytes());

        buf.freeze()
    }
}

impl FromBytes for CompactString {
    fn from_be_bytes<B: Buf>(buf: &mut B) -> Result<Self> {
        let len = buf.try_get_u8()? - 1; // Adjust length to match the protocol

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
    pub fn new(array: Vec<T>) -> Self {
        Self { array }
    }

    pub fn default() -> Self {
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
    T: ToBytes + std::fmt::Debug,
{
    fn to_be_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        if self.array.is_empty() {
            buf.put_u8(0);
            return buf.freeze();
        }

        let len = self.array.len() as u8;
        buf.put_u8(len + 1); // +1 to match the protocol

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
        let len = buf.try_get_u8()?;

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
