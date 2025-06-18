use crate::Result;

use super::{
    bytes::{FromBytes, ToBytes},
    error::IoError,
};

#[derive(Debug)]
pub struct NullableString {
    value: Option<String>,
}

impl ToBytes for NullableString {
    fn to_be_bytes(&self) -> Vec<u8> {
        if self.value.is_none() {
            (-1_i16).to_be_bytes().to_vec() // -1 for empty string
        } else {
            let value = self.value.as_ref().unwrap();
            let mut bytes = (value.len() as i16).to_be_bytes().to_vec();
            bytes.extend_from_slice(value.as_bytes());

            bytes
        }
    }
}

impl FromBytes for NullableString {
    fn from_be_bytes<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut len_buf = [0u8; 2];
        reader.read_exact(&mut len_buf)?;

        let len = i16::from_be_bytes(len_buf);
        if len < 0 {
            Ok(NullableString { value: None })
        } else {
            let mut str_buf = vec![0u8; len as usize];
            reader.read_exact(&mut str_buf)?;

            Ok(NullableString {
                value: Some(String::from_utf8(str_buf)?),
            })
        }
    }
}

#[derive(Debug)]
pub struct CompactString {
    value: String,
}

impl ToBytes for CompactString {
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        if self.value.is_empty() {
            // If the string is empty, we just write a single byte 0
            bytes.push(0);
            return bytes;
        }

        let len = self.value.len() as u8;
        bytes.push(len + 1); // +1 to match the protocol
        bytes.extend_from_slice(self.value.as_bytes());

        bytes
    }
}

impl FromBytes for CompactString {
    fn from_be_bytes<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut len_buf = [0u8; 1];

        if reader.read_exact(&mut len_buf).is_err() {
            return Err(IoError::new("failed to read CompactString length").into());
        }

        // len_buf[0] is the length of the string, minus 1
        let len = u8::from_be_bytes(len_buf) - 1;

        let mut str_buf = vec![0u8; len as usize];
        reader.read_exact(&mut str_buf)?;

        Ok(CompactString {
            value: String::from_utf8(str_buf)?,
        })
    }
}

#[derive(Debug)]
pub struct CompactArray<T> {
    array: Vec<T>,
}

impl<T> CompactArray<T> {
    pub fn new(array: Vec<T>) -> Self {
        Self { array }
    }
}

impl<T> ToBytes for CompactArray<T>
where
    T: ToBytes + std::fmt::Debug,
{
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        if self.array.is_empty() {
            // If the array is empty, we just write a single byte 0
            bytes.push(0);
            return bytes;
        }

        let len = self.array.len() as u8;
        bytes.push(len + 1); // +1 to match the protocol
        for item in &self.array {
            bytes.extend_from_slice(&item.to_be_bytes());
        }

        bytes
    }
}

impl<T> FromBytes for CompactArray<T>
where
    T: FromBytes,
{
    fn from_be_bytes<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut len_buf = [0u8; 1];

        reader.read_exact(&mut len_buf)?;

        let len = u8::from_be_bytes(len_buf);

        match len {
            0 => Ok(CompactArray { array: Vec::new() }),
            _ => {
                let len = len - 1; // Adjust length to match the protocol
                let mut array = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    array.push(T::from_be_bytes(reader)?);
                }
                Ok(CompactArray { array })
            }
        }
    }
}
