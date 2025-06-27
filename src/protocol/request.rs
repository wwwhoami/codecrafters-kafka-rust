use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::Result;

use super::{
    bytes::{FromBytes, ToBytes},
    primitives::{ApiKey, CompactArray, CompactString, NullableString},
};

#[derive(Debug)]
pub struct RequestHeaderV2 {
    request_api_key: ApiKey,
    request_api_version: i16,
    correlation_id: i32,
    client_id: NullableString,
    tag: CompactArray<NullableString>,
}

impl RequestHeaderV2 {
    pub fn request_api_version(&self) -> i16 {
        self.request_api_version
    }

    pub fn correlation_id(&self) -> i32 {
        self.correlation_id
    }

    pub fn request_api_key(&self) -> &ApiKey {
        &self.request_api_key
    }
}

impl ToBytes for RequestHeaderV2 {
    fn to_be_bytes(&self) -> Bytes {
        use bytes::BufMut;

        let mut buf = BytesMut::new();

        buf.extend_from_slice(&self.request_api_key.to_be_bytes());
        buf.put_i16(self.request_api_version);
        buf.put_i32(self.correlation_id);
        buf.extend_from_slice(&self.client_id.to_be_bytes());
        buf.extend_from_slice(&self.tag.to_be_bytes());

        buf.freeze()
    }
}
impl FromBytes for RequestHeaderV2 {
    fn from_be_bytes<B: Buf>(mut buf: &mut B) -> Result<Self> {
        let request_api_key = ApiKey::from_be_bytes(&mut buf)
            .map_err(|e| anyhow::anyhow!("failed to parse request_api_key: {}", e))?;

        let request_api_version = buf
            .try_get_i16()
            .map_err(|e| anyhow::anyhow!("failed to parse i16 for request_api_version: {}", e))?;

        let correlation_id = buf
            .try_get_i32()
            .map_err(|e| anyhow::anyhow!("failed to parse i32 for correlation_id: {}", e))?;

        let client_id = NullableString::from_be_bytes(&mut buf)
            .map_err(|e| anyhow::anyhow!("failed to parse NullableString for client_id: {}", e))?;
        let tag = CompactArray::<NullableString>::from_be_bytes(&mut buf).map_err(|e| {
            anyhow::anyhow!(
                "failed to parse CompactArray<NullableString> for tag: {}",
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
pub enum RequestBody {
    ApiVersionsRequestV4(ApiVersionsRequestV4),
    DescribeTopicPartitionsRequestV0(DescribeTopicPartitionsRequestV0),
    FetchRequestV4(FetchRequestV4),
}

impl RequestBody {
    pub fn as_describe_topic_partitions_request_v0(
        &self,
    ) -> Option<&DescribeTopicPartitionsRequestV0> {
        if let Self::DescribeTopicPartitionsRequestV0(v) = self {
            Some(v)
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct RequestV0 {
    message_size: i32,
    header: RequestHeaderV2,
    body: RequestBody,
}

impl RequestV0 {
    pub fn header(&self) -> &RequestHeaderV2 {
        &self.header
    }

    pub fn body(&self) -> &RequestBody {
        &self.body
    }
}

impl ToBytes for RequestV0 {
    fn to_be_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        buf.put_i32(self.message_size);
        buf.extend_from_slice(&self.header.to_be_bytes());

        buf.freeze()
    }
}

impl FromBytes for RequestV0 {
    fn from_be_bytes<B: bytes::Buf>(mut buf: &mut B) -> Result<Self> {
        let message_size = buf
            .try_get_i32()
            .map_err(|e| anyhow::anyhow!("failed to parse i32 for message_size: {}", e))?;

        let header = RequestHeaderV2::from_be_bytes(&mut buf)
            .map_err(|e| anyhow::anyhow!("failed to parse RequestHeaderV2: {}", e))?;

        let body = match header.request_api_key {
            ApiKey::ApiVersions => RequestBody::ApiVersionsRequestV4(
                ApiVersionsRequestV4::from_be_bytes(&mut buf)
                    .map_err(|e| anyhow::anyhow!("failed to parse ApiVersionsRequestV4: {}", e))?,
            ),
            ApiKey::DescribeTopicPartitions => RequestBody::DescribeTopicPartitionsRequestV0(
                DescribeTopicPartitionsRequestV0::from_be_bytes(&mut buf).map_err(|e| {
                    anyhow::anyhow!("failed to parse DescribeTopicPartitionsRequestV0: {}", e)
                })?,
            ),
            ApiKey::Fetch => RequestBody::FetchRequestV4(
                FetchRequestV4::from_be_bytes(&mut buf)
                    .map_err(|e| anyhow::anyhow!("failed to parse FetchRequestV4: {}", e))?,
            ),
        };

        Ok(RequestV0 {
            message_size,
            header,
            body,
        })
    }
}

#[derive(Debug)]
pub struct ApiVersionsRequestV4 {
    client_software_name: CompactString,
    client_software_version: CompactString,
    tag: CompactArray<NullableString>,
}

impl FromBytes for ApiVersionsRequestV4 {
    fn from_be_bytes<B: bytes::Buf>(buf: &mut B) -> Result<Self> {
        let client_software_name = CompactString::from_be_bytes(buf).map_err(|e| {
            anyhow::anyhow!(
                "failed to parse CompactString for client_software_name: {}",
                e
            )
        })?;
        let client_software_version = CompactString::from_be_bytes(buf).map_err(|e| {
            anyhow::anyhow!(
                "failed to parse CompactString for client_software_version: {}",
                e
            )
        })?;
        let tag = CompactArray::<NullableString>::from_be_bytes(buf).map_err(|e| {
            anyhow::anyhow!(
                "failed to parse CompactArray<NullableString> for tag: {}",
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

#[derive(Debug)]
pub struct DescribeTopicPartitionsRequestV0 {
    topics: CompactArray<Topic>,
    response_partiotion_limit: i32,
    cursor: u8,
    tag: CompactArray<NullableString>,
}

impl DescribeTopicPartitionsRequestV0 {
    pub fn topics(&self) -> &CompactArray<Topic> {
        &self.topics
    }
}

impl FromBytes for DescribeTopicPartitionsRequestV0 {
    fn from_be_bytes<B: bytes::Buf>(buf: &mut B) -> Result<Self> {
        let topics = CompactArray::<Topic>::from_be_bytes(buf).map_err(|e| {
            anyhow::anyhow!("failed to parse CompactArray<Topic> for topics: {}", e)
        })?;

        let response_partition_limit = buf.try_get_i32().map_err(|e| {
            anyhow::anyhow!("failed to parse i32 for response_partition_limit: {}", e)
        })?;

        let cursor = buf
            .try_get_u8()
            .map_err(|e| anyhow::anyhow!("failed to parse u8 for cursor: {}", e))?;

        let tag = CompactArray::<NullableString>::from_be_bytes(buf).map_err(|e| {
            anyhow::anyhow!(
                "failed to parse CompactArray<NullableString> for tag: {}",
                e
            )
        })?;

        Ok(DescribeTopicPartitionsRequestV0 {
            topics,
            response_partiotion_limit: response_partition_limit,
            cursor,
            tag,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Topic {
    topic: CompactString,
    tag: CompactArray<NullableString>,
}

impl Topic {
    pub fn topic(&self) -> &str {
        &self.topic.as_str()
    }
}

impl FromBytes for Topic {
    fn from_be_bytes<B: bytes::Buf>(buf: &mut B) -> Result<Self> {
        let topic = CompactString::from_be_bytes(buf)
            .map_err(|e| anyhow::anyhow!("failed to parse CompactString for topic: {}", e))?;
        let tag = CompactArray::<NullableString>::from_be_bytes(buf).map_err(|e| {
            anyhow::anyhow!(
                "failed to parse CompactArray<NullableString> for tag: {}",
                e
            )
        })?;

        Ok(Topic { topic, tag })
    }
}

#[derive(Debug)]
pub struct FetchRequestV4 {
    replica_id: i32,
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    isolation_level: i8,
    topics: CompactArray<TopicsPartitions>,
}

impl FromBytes for FetchRequestV4 {
    fn from_be_bytes<B: bytes::Buf>(buf: &mut B) -> Result<Self> {
        let replica_id = buf
            .try_get_i32()
            .map_err(|e| anyhow::anyhow!("failed to parse i32 for replica_id: {}", e))?;

        let max_wait_ms = buf
            .try_get_i32()
            .map_err(|e| anyhow::anyhow!("failed to parse i32 for max_wait_ms: {}", e))?;

        let min_bytes = buf
            .try_get_i32()
            .map_err(|e| anyhow::anyhow!("failed to parse i32 for min_bytes: {}", e))?;

        let max_bytes = buf
            .try_get_i32()
            .map_err(|e| anyhow::anyhow!("failed to parse i32 for max_bytes: {}", e))?;

        let isolation_level = buf
            .try_get_i8()
            .map_err(|e| anyhow::anyhow!("failed to parse i8 for isolation_level: {}", e))?;

        let topics = CompactArray::<TopicsPartitions>::from_be_bytes(buf).map_err(|e| {
            anyhow::anyhow!("failed to parse CompactArray<TopicsPartitions>: {}", e)
        })?;

        Ok(FetchRequestV4 {
            replica_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            topics,
        })
    }
}

#[derive(Debug)]
pub struct TopicsPartitions {
    topic: CompactString,
    partitions: CompactArray<Partition>,
}

impl FromBytes for TopicsPartitions {
    fn from_be_bytes<B: bytes::Buf>(buf: &mut B) -> Result<Self> {
        let topic = CompactString::from_be_bytes(buf)
            .map_err(|e| anyhow::anyhow!("failed to parse CompactString for topic: {}", e))?;

        let partitions = CompactArray::<Partition>::from_be_bytes(buf)
            .map_err(|e| anyhow::anyhow!("failed to parse CompactArray<Partition>: {}", e))?;

        Ok(TopicsPartitions { topic, partitions })
    }
}

#[derive(Debug)]
pub struct Partition {
    partition: i32,
    fetch_offset: i64,
    partition_max_bytes: i32,
}

impl FromBytes for Partition {
    fn from_be_bytes<B: bytes::Buf>(buf: &mut B) -> Result<Self> {
        let partition = buf
            .try_get_i32()
            .map_err(|e| anyhow::anyhow!("failed to parse i32 for partition: {}", e))?;

        let fetch_offset = buf
            .try_get_i64()
            .map_err(|e| anyhow::anyhow!("failed to parse i64 for fetch_offset: {}", e))?;

        let partition_max_bytes = buf
            .try_get_i32()
            .map_err(|e| anyhow::anyhow!("failed to parse i32 for partition_max_bytes: {}", e))?;

        Ok(Partition {
            partition,
            fetch_offset,
            partition_max_bytes,
        })
    }
}
