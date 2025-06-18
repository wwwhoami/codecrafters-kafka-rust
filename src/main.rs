use std::{
    io::{Cursor, Read, Write},
    net::TcpListener,
};

use anyhow::Error;

#[derive(Debug)]
struct NullableString {
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
    fn from_be_bytes<R: std::io::Read>(reader: &mut R) -> Result<Self, Error> {
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
struct CompactString {
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
    fn from_be_bytes<R: std::io::Read>(reader: &mut R) -> Result<Self, Error> {
        let mut len_buf = [0u8; 1];

        if reader.read_exact(&mut len_buf).is_err() {
            return Err(anyhow::anyhow!("Failed to read CompactString length"));
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
struct CompactArray<T> {
    array: Vec<T>,
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
    fn from_be_bytes<R: std::io::Read>(reader: &mut R) -> Result<Self, Error> {
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

        // if zero_len_buf[0] == 0 {
        //     // If the first byte is 0, we treat it as an empty array
        //     return Ok(CompactArray { array: Vec::new() });
        // }
        //
        // // Else, we read the remaining length of the array
        // reader.read_exact(&mut remaining_len_buf)?;
        //
        // let len_buf = [zero_len_buf[0], remaining_len_buf[0]];
        // let len = u16::from_be_bytes(len_buf);
        //
        // let mut array = Vec::with_capacity(len as usize);
        // for _ in 0..len {
        //     array.push(T::from_be_bytes(reader)?);
        // }
        //
        // Ok(CompactArray { array })
    }
}

trait ToBytes {
    fn to_be_bytes(&self) -> Vec<u8>;
}

trait FromBytes: Sized {
    fn from_be_bytes<R: std::io::Read>(reader: &mut R) -> Result<Self, Error>;
}

#[derive(Debug, Clone)]
struct UnsupportedApiKeyError {
    key: i16,
}

impl std::fmt::Display for UnsupportedApiKeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unsupported API key {}", self.key)
    }
}

impl std::error::Error for UnsupportedApiKeyError {}

// #[derive(Debug)]
// enum RequestApiKey {
//     Fetch = 1,
//     ApiVersions = 18,
// }
//
// impl ToBytes for RequestApiKey {
//     fn to_be_bytes(&self) -> Vec<u8> {
//         match self {
//             RequestApiKey::Fetch => (1_i16).to_be_bytes().to_vec(),
//             RequestApiKey::ApiVersions => (18_i16).to_be_bytes().to_vec(),
//         }
//     }
// }
//
// impl FromBytes for RequestApiKey {
//     fn from_be_bytes<R: std::io::Read>(reader: &mut R) -> Result<Self, Error> {
//         let mut buf = [0u8; 2];
//         reader.read_exact(&mut buf)?;
//
//         let key = i16::from_be_bytes(buf);
//         match key {
//             1 => Ok(RequestApiKey::Fetch),
//             18 => Ok(RequestApiKey::ApiVersions),
//             _ => Err(UnsupportedApiKeyError { key }.into()),
//         }
//     }
// }

#[derive(Debug)]
struct RequestHeaderV2 {
    request_api_key: i16,
    request_api_version: i16,
    correlation_id: i32,
    client_id: NullableString,
    tag: CompactArray<NullableString>,
}

impl RequestHeaderV2 {
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.request_api_key.to_be_bytes());
        bytes.extend_from_slice(&self.request_api_version.to_be_bytes());
        bytes.extend_from_slice(&self.correlation_id.to_be_bytes());

        bytes.extend_from_slice(&self.client_id.to_be_bytes());
        bytes.extend_from_slice(&self.tag.to_be_bytes());

        bytes
    }

    fn from_be_bytes<R: std::io::Read>(mut reader: &mut R) -> Result<Self, Error> {
        let mut buf2 = [0u8; 2];
        let mut buf4 = [0u8; 4];

        reader
            .read_exact(&mut buf2)
            .map_err(|e| anyhow::anyhow!("Failed to read request_api_key: {}", e))?;
        let request_api_key = i16::from_be_bytes(buf2);

        reader
            .read_exact(&mut buf2)
            .map_err(|e| anyhow::anyhow!("Failed to read request_api_version: {}", e))?;
        let request_api_version = i16::from_be_bytes(buf2);

        reader
            .read_exact(&mut buf4)
            .map_err(|e| anyhow::anyhow!("Failed to read correlation_id: {}", e))?;
        let correlation_id = i32::from_be_bytes(buf4);

        let client_id = NullableString::from_be_bytes(&mut reader)
            .map_err(|e| anyhow::anyhow!("Failed to parse NullableString for client_id: {}", e))?;
        let tag = CompactArray::<NullableString>::from_be_bytes(&mut reader).map_err(|e| {
            anyhow::anyhow!(
                "Failed to parse CompactArray<NullableString> for tag: {}",
                e
            )
        })?;

        Ok(RequestHeaderV2 {
            request_api_key,
            request_api_version,
            correlation_id,
            client_id,
            tag,
        })
    }
}

#[derive(Debug)]
enum RequestBody {
    ApiVersionsRequestV4(ApiVersionsRequestV4),
    UnsupportedApiKey(UnsupportedApiKeyError),
}

#[derive(Debug)]
struct RequestV0 {
    message_size: i32,
    header: RequestHeaderV2,
    body: RequestBody,
}

#[derive(Debug, Clone)]
enum BodyError {
    UnsupportedApiKey,
}

impl std::error::Error for BodyError {}

impl std::fmt::Display for BodyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BodyError::UnsupportedApiKey => write!(f, "unsupported API key"),
        }
    }
}

impl RequestV0 {
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        bytes.extend_from_slice(&self.message_size.to_be_bytes());
        bytes.extend_from_slice(&self.header.to_be_bytes());

        bytes
    }

    fn from_be_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let mut rdr = Cursor::new(bytes);

        let mut buf4 = [0u8; 4];

        rdr.read_exact(&mut buf4)
            .map_err(|e| anyhow::anyhow!("Failed to read message size: {}", e))?;

        let message_size = i32::from_be_bytes(buf4);

        let header = RequestHeaderV2::from_be_bytes(&mut rdr)
            .map_err(|e| anyhow::anyhow!("Failed to parse RequestHeaderV2: {}", e))?;

        let body = match header.request_api_key {
            18 => RequestBody::ApiVersionsRequestV4(
                ApiVersionsRequestV4::from_be_bytes(&mut rdr)
                    .map_err(|e| anyhow::anyhow!("Failed to parse ApiVersionsRequestV4: {}", e))?,
            ),
            _ => RequestBody::UnsupportedApiKey(UnsupportedApiKeyError {
                key: header.request_api_key,
            }),
        };

        Ok(RequestV0 {
            message_size,
            header,
            body,
        })
    }
}

#[derive(Debug)]
struct ApiVersionsRequestV4 {
    client_software_name: CompactString,
    client_software_version: CompactString,
    tag: CompactArray<NullableString>,
}

impl FromBytes for ApiVersionsRequestV4 {
    fn from_be_bytes<R: std::io::Read>(reader: &mut R) -> Result<Self, Error> {
        let client_software_name = CompactString::from_be_bytes(reader).map_err(|e| {
            anyhow::anyhow!(
                "Failed to parse CompactString for client_software_name: {}",
                e
            )
        })?;
        let client_software_version = CompactString::from_be_bytes(reader).map_err(|e| {
            anyhow::anyhow!(
                "Failed to parse CompactString for client_software_version: {}",
                e
            )
        })?;
        let tag = CompactArray::<NullableString>::from_be_bytes(reader).map_err(|e| {
            anyhow::anyhow!(
                "Failed to parse CompactArray<NullableString> for tag: {}",
                e
            )
        })?;

        Ok(ApiVersionsRequestV4 {
            client_software_name,
            client_software_version,
            tag,
        })
    }
}

#[derive(Debug, Clone)]
enum ErrorCode {
    None = 0,
    UnknownServerError = -1,
    UnsupportedVersion = 35,
}

#[derive(Debug)]
struct ApiVersionsResponseBodyV4 {
    error_code: ErrorCode,
    api_versions: CompactArray<ApiVersion>,
    throttle_time_ms: i32,
    tag: CompactArray<NullableString>,
}

impl ApiVersionsResponseBodyV4 {
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        bytes.extend_from_slice(&(self.error_code.clone() as i16).to_be_bytes());
        bytes.extend_from_slice(&self.api_versions.to_be_bytes());
        bytes.extend_from_slice(&self.throttle_time_ms.to_be_bytes());
        bytes.extend_from_slice(&self.tag.to_be_bytes());

        bytes
    }
}

#[derive(Debug)]
struct ApiVersion {
    api_key: i16,
    min_version: i16,
    max_version: i16,
    tag: CompactArray<NullableString>,
}

impl ToBytes for ApiVersion {
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        bytes.extend_from_slice(&self.api_key.to_be_bytes());
        bytes.extend_from_slice(&self.min_version.to_be_bytes());
        bytes.extend_from_slice(&self.max_version.to_be_bytes());
        bytes.extend_from_slice(&self.tag.to_be_bytes());

        bytes
    }
}

#[derive(Debug)]
struct ResponseV0 {
    message_size: i32,
    header: ResponseHeaderV2,
    body: ResponseBody,
}

impl ToBytes for ResponseV0 {
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        bytes.extend_from_slice(&self.message_size.to_be_bytes());
        bytes.extend_from_slice(&self.header.correlation_id.to_be_bytes());
        bytes.extend_from_slice(&self.body.to_be_bytes());

        bytes
    }
}

#[derive(Debug)]
struct ResponseHeaderV2 {
    correlation_id: i32,
}

impl ToBytes for ResponseHeaderV2 {
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.correlation_id.to_be_bytes());
        bytes
    }
}

#[derive(Debug)]
enum ResponseBody {
    ApiVersionsResponseV4(ApiVersionsResponseBodyV4),
}

impl ToBytes for ResponseBody {
    fn to_be_bytes(&self) -> Vec<u8> {
        match self {
            ResponseBody::ApiVersionsResponseV4(body) => body.to_be_bytes(),
        }
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").expect("unable to bind to port");

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut buf = [0; 1024]; // Buffer to hold incoming data

                match stream.read(&mut buf) {
                    Ok(0) => {
                        println!("connection closed by client");
                        continue;
                    }
                    Ok(n) => {
                        println!("received {} bytes from client", n);

                        let request = match RequestV0::from_be_bytes(&buf[..n]) {
                            Ok(req) => req,
                            Err(e) => {
                                eprintln!("error parsing request: {}", e);
                                continue;
                            }
                        };
                        println!("parsed request: {:?}", request);

                        let response_body = match request.header.request_api_version {
                            4 => {
                                // RequestApiKey::ApiVersions
                                ApiVersionsResponseBodyV4 {
                                    error_code: ErrorCode::None,
                                    api_versions: CompactArray {
                                        array: vec![ApiVersion {
                                            api_key: 18,
                                            min_version: 0,
                                            max_version: 4,
                                            tag: CompactArray { array: vec![] },
                                        }],
                                    },
                                    throttle_time_ms: 0,
                                    tag: CompactArray { array: vec![] },
                                }
                            }
                            _ => ApiVersionsResponseBodyV4 {
                                error_code: ErrorCode::UnsupportedVersion,
                                api_versions: CompactArray { array: vec![] },
                                throttle_time_ms: 0,
                                tag: CompactArray { array: vec![] },
                            },
                        };

                        let response_header = ResponseHeaderV2 {
                            correlation_id: request.header.correlation_id,
                        };
                        let response = ResponseV0 {
                            message_size: response_body.to_be_bytes().len() as i32
                                + response_header.to_be_bytes().len() as i32,
                            header: response_header,
                            body: ResponseBody::ApiVersionsResponseV4(response_body),
                        };

                        stream
                            .write_all(&response.to_be_bytes())
                            .expect("unable to write response to stream");
                        stream.flush().expect("unable to flush stream");

                        // println!("response message_size: {:?}", response.message_size);
                        // println!("response sent to client: {:?}", response);
                        // println!("response size: {}", response.to_be_bytes().len());
                        // println!("response as bytes: {:?}", response.to_be_bytes());
                    }
                    Err(e) => {
                        eprintln!("error reading from stream: {}", e);
                        continue;
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
