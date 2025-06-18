use super::{
    bytes::ToBytes,
    primitives::{CompactArray, NullableString},
};

#[derive(Debug, Clone)]
pub enum ErrorCode {
    None = 0,
    UnknownServerError = -1,
    UnsupportedVersion = 35,
}

#[derive(Debug)]
pub struct ResponseV0 {
    message_size: i32,
    header: ResponseHeaderV2,
    body: ResponseBody,
}

impl ResponseV0 {
    pub fn new(message_size: i32, header: ResponseHeaderV2, body: ResponseBody) -> Self {
        Self {
            message_size,
            header,
            body,
        }
    }
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
pub struct ResponseHeaderV2 {
    correlation_id: i32,
}

impl ResponseHeaderV2 {
    pub fn new(correlation_id: i32) -> Self {
        Self { correlation_id }
    }
}

impl ToBytes for ResponseHeaderV2 {
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.correlation_id.to_be_bytes());
        bytes
    }
}

#[derive(Debug)]
pub enum ResponseBody {
    ApiVersionsResponseV4(ApiVersionsResponseBodyV4),
}

impl ToBytes for ResponseBody {
    fn to_be_bytes(&self) -> Vec<u8> {
        match self {
            ResponseBody::ApiVersionsResponseV4(body) => body.to_be_bytes(),
        }
    }
}

#[derive(Debug)]
pub struct ApiVersionsResponseBodyV4 {
    pub error_code: ErrorCode,
    pub api_versions: CompactArray<ApiVersion>,
    pub throttle_time_ms: i32,
    pub tag: CompactArray<NullableString>,
}

impl ApiVersionsResponseBodyV4 {
    pub fn new(
        error_code: ErrorCode,
        api_versions: CompactArray<ApiVersion>,
        throttle_time_ms: i32,
        tag: CompactArray<NullableString>,
    ) -> Self {
        Self {
            error_code,
            api_versions,
            throttle_time_ms,
            tag,
        }
    }
}

impl ToBytes for ApiVersionsResponseBodyV4 {
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
pub struct ApiVersion {
    api_key: i16,
    min_version: i16,
    max_version: i16,
    tag: CompactArray<NullableString>,
}

impl ApiVersion {
    pub fn new(
        api_key: i16,
        min_version: i16,
        max_version: i16,
        tag: CompactArray<NullableString>,
    ) -> Self {
        Self {
            api_key,
            min_version,
            max_version,
            tag,
        }
    }
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
