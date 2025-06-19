use uuid::Uuid;

use super::{
    bytes::ToBytes,
    primitives::{ApiKey, CompactArray, CompactString, NullableString},
};

#[derive(Debug, Clone)]
pub enum ErrorCode {
    None = 0,
    UnknownServerError = -1,
    UnsupportedVersion = 35,
    UnknownTopicOrPartition = 3,
}

#[derive(Debug)]
pub struct ResponseV0 {
    message_size: i32,
    header: ResponseHeader,
    body: ResponseBody,
}

impl ResponseV0 {
    pub fn new(message_size: i32, header: ResponseHeader, body: ResponseBody) -> Self {
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
        bytes.extend_from_slice(&self.header.to_be_bytes());
        bytes.extend_from_slice(&self.body.to_be_bytes());

        bytes
    }
}

#[derive(Debug)]
pub enum ResponseHeader {
    V0(ResponseHeaderV0),
    V1(ResponseHeaderV1),
}

impl ToBytes for ResponseHeader {
    fn to_be_bytes(&self) -> Vec<u8> {
        match self {
            ResponseHeader::V0(header) => header.to_be_bytes(),
            ResponseHeader::V1(header) => header.to_be_bytes(),
        }
    }
}

#[derive(Debug)]
pub struct ResponseHeaderV0 {
    correlation_id: i32,
}

impl ResponseHeaderV0 {
    pub fn new(correlation_id: i32) -> Self {
        Self { correlation_id }
    }
}

impl ToBytes for ResponseHeaderV0 {
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        bytes.extend_from_slice(&self.correlation_id.to_be_bytes());

        bytes
    }
}

#[derive(Debug)]
pub struct ResponseHeaderV1 {
    correlation_id: i32,
    tag: CompactArray<NullableString>,
}

impl ResponseHeaderV1 {
    pub fn new(correlation_id: i32) -> Self {
        Self {
            correlation_id,
            tag: CompactArray::default(),
        }
    }
}

impl ToBytes for ResponseHeaderV1 {
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        bytes.extend_from_slice(&self.correlation_id.to_be_bytes());
        bytes.extend_from_slice(&self.tag.to_be_bytes());

        bytes
    }
}

#[derive(Debug)]
pub enum ResponseBody {
    ApiVersionsResponseV4(ApiVersionsResponseBodyV4),
    DescribeTopicPartiotionsResponseV0(DescribeTopicPartiotionsResponseBodyV0),
}

impl ToBytes for ResponseBody {
    fn to_be_bytes(&self) -> Vec<u8> {
        match self {
            ResponseBody::ApiVersionsResponseV4(body) => body.to_be_bytes(),
            ResponseBody::DescribeTopicPartiotionsResponseV0(body) => body.to_be_bytes(),
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
    api_key: ApiKey,
    min_version: i16,
    max_version: i16,
    tag: CompactArray<NullableString>,
}

impl ApiVersion {
    pub fn new(
        api_key: ApiKey,
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

#[derive(Debug)]
pub struct DescribeTopicPartiotionsResponseBodyV0 {
    throttle_time_ms: i32,
    topics: CompactArray<Topic>,
    next_cursor: u8,
    tag: CompactArray<NullableString>,
}

impl DescribeTopicPartiotionsResponseBodyV0 {
    pub fn new(
        throttle_time_ms: i32,
        topics: CompactArray<Topic>,
        next_cursor: u8,
        tag: CompactArray<NullableString>,
    ) -> Self {
        Self {
            throttle_time_ms,
            topics,
            next_cursor,
            tag,
        }
    }
}

impl ToBytes for DescribeTopicPartiotionsResponseBodyV0 {
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        bytes.extend_from_slice(&self.throttle_time_ms.to_be_bytes());
        bytes.extend_from_slice(&self.topics.to_be_bytes());
        bytes.push(self.next_cursor);
        bytes.extend_from_slice(&self.tag.to_be_bytes());

        bytes
    }
}

#[derive(Debug)]
pub struct Topic {
    error_code: ErrorCode,
    name: CompactString,
    id: Uuid,
    is_internal: bool,
    partitions: CompactArray<NullableString>,
    authorized_operations: [u8; 4],
    tag: CompactArray<NullableString>,
}

impl Topic {
    pub fn new(
        error_code: ErrorCode,
        name: CompactString,
        id: Uuid,
        is_internal: bool,
        partitions: CompactArray<NullableString>,
        authorized_operations: [u8; 4],
        tag: CompactArray<NullableString>,
    ) -> Self {
        Self {
            error_code,
            name,
            id,
            is_internal,
            partitions,
            authorized_operations,
            tag,
        }
    }
}

impl ToBytes for Topic {
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        bytes.extend_from_slice(&(self.error_code.clone() as i16).to_be_bytes());
        bytes.extend_from_slice(&self.name.to_be_bytes());
        bytes.extend_from_slice(self.id.as_bytes());
        bytes.push(self.is_internal as u8);
        bytes.extend_from_slice(&self.partitions.to_be_bytes());
        bytes.extend_from_slice(&self.authorized_operations);
        bytes.extend_from_slice(&self.tag.to_be_bytes());

        bytes
    }
}
