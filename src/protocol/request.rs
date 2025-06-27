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
    FetchRequestV16(FetchRequestV16),
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

    pub fn as_fetch_request_v16(&self) -> Option<&FetchRequestV16> {
        if let Self::FetchRequestV16(v) = self {
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
            ApiKey::Fetch => RequestBody::FetchRequestV16(
                FetchRequestV16::from_be_bytes(&mut buf)
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

    pub fn topic_names(&self) -> Vec<String> {
        self.topics
            .to_vec()
            .iter()
            .map(|topic| topic.topic().to_string())
            .collect()
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

impl Default for DescribeTopicPartitionsRequestV0 {
    fn default() -> Self {
        DescribeTopicPartitionsRequestV0 {
            topics: CompactArray::new(),
            response_partiotion_limit: 0,
            cursor: u8::MAX,
            tag: CompactArray::new(),
        }
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

#[derive(Debug, Clone)]
pub struct FetchRequestV16 {
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    isolation_level: i8,
    session_id: i32,
    session_epoch: i32,
    topics: CompactArray<TopicsPartitions>,
    forgotten_topics: CompactArray<ForgottenTopic>,
    rack_id: CompactString,
}

impl FetchRequestV16 {
    pub fn topics(&self) -> &CompactArray<TopicsPartitions> {
        &self.topics
    }
}

impl Default for FetchRequestV16 {
    fn default() -> Self {
        FetchRequestV16 {
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1048576,
            isolation_level: 0,
            session_id: -1,
            session_epoch: -1,
            topics: CompactArray::new(),
            forgotten_topics: CompactArray::new(),
            rack_id: CompactString::default(),
        }
    }
}

impl FromBytes for FetchRequestV16 {
    fn from_be_bytes<B: bytes::Buf>(buf: &mut B) -> Result<Self> {
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

        let session_id = buf
            .try_get_i32()
            .map_err(|e| anyhow::anyhow!("failed to parse i32 for session_id: {}", e))?;

        let session_epoch = buf
            .try_get_i32()
            .map_err(|e| anyhow::anyhow!("failed to parse i32 for session_epoch: {}", e))?;

        let topics = CompactArray::<TopicsPartitions>::from_be_bytes(buf).map_err(|e| {
            anyhow::anyhow!("failed to parse CompactArray<TopicsPartitions>: {}", e)
        })?;

        let forgotten_topics = CompactArray::<ForgottenTopic>::from_be_bytes(buf)
            .map_err(|e| anyhow::anyhow!("failed to parse CompactArray<ForgottenTopic>: {}", e))?;

        let rack_id = CompactString::from_be_bytes(buf)
            .map_err(|e| anyhow::anyhow!("failed to parse CompactString for rack_id: {}", e))?;

        Ok(FetchRequestV16 {
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics,
            rack_id,
        })
    }
}

#[derive(Debug, Clone)]
pub struct TopicsPartitions {
    topic_id: uuid::Uuid,
    partitions: CompactArray<Partition>,
    tag: CompactArray<NullableString>,
}

impl TopicsPartitions {
    pub fn topic_id(&self) -> uuid::Uuid {
        self.topic_id
    }
}

impl Default for TopicsPartitions {
    fn default() -> Self {
        TopicsPartitions {
            topic_id: uuid::Uuid::nil(),
            partitions: CompactArray::new(),
            tag: CompactArray::new(),
        }
    }
}

impl FromBytes for TopicsPartitions {
    fn from_be_bytes<B: bytes::Buf>(buf: &mut B) -> Result<Self> {
        let mut buf16 = [0u8; 16];
        buf.copy_to_slice(&mut buf16);

        let topic_id = uuid::Uuid::from_slice(&buf16)
            .map_err(|e| anyhow::anyhow!("failed to parse Uuid for topic_id: {}", e))?;

        let partitions = CompactArray::<Partition>::from_be_bytes(buf)
            .map_err(|e| anyhow::anyhow!("failed to parse CompactArray<Partition>: {}", e))?;

        let tag = CompactArray::<NullableString>::from_be_bytes(buf).map_err(|e| {
            anyhow::anyhow!(
                "failed to parse CompactArray<NullableString> for tag: {}",
                e
            )
        })?;

        Ok(TopicsPartitions {
            topic_id,
            partitions,
            tag,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Partition {
    partition: i32,
    current_leader_epoch: i32,
    fetch_offset: i64,
    last_fetched_epoch: i32,
    log_start_offset: i64,
    partition_max_bytes: i32,
}

impl FromBytes for Partition {
    fn from_be_bytes<B: bytes::Buf>(buf: &mut B) -> Result<Self> {
        let partition = buf
            .try_get_i32()
            .map_err(|e| anyhow::anyhow!("failed to parse i32 for partition: {}", e))?;

        let current_leader_epoch = buf
            .try_get_i32()
            .map_err(|e| anyhow::anyhow!("failed to parse i32 for current_leader_epoch: {}", e))?;

        let fetch_offset = buf
            .try_get_i64()
            .map_err(|e| anyhow::anyhow!("failed to parse i64 for fetch_offset: {}", e))?;

        let last_fetched_epoch = buf
            .try_get_i32()
            .map_err(|e| anyhow::anyhow!("failed to parse i32 for last_fetched_epoch: {}", e))?;

        let log_start_offset = buf
            .try_get_i64()
            .map_err(|e| anyhow::anyhow!("failed to parse i64 for log_start_offset: {}", e))?;

        let partition_max_bytes = buf
            .try_get_i32()
            .map_err(|e| anyhow::anyhow!("failed to parse i32 for partition_max_bytes: {}", e))?;

        Ok(Partition {
            partition,
            current_leader_epoch,
            fetch_offset,
            last_fetched_epoch,
            log_start_offset,
            partition_max_bytes,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ForgottenTopic {
    topic_id: uuid::Uuid,
    partitions: i32,
}

impl FromBytes for ForgottenTopic {
    fn from_be_bytes<B: bytes::Buf>(buf: &mut B) -> Result<Self> {
        let mut buf16 = [0u8; 16];
        buf.copy_to_slice(&mut buf16);

        let topic_id = uuid::Uuid::from_slice(&buf16)
            .map_err(|e| anyhow::anyhow!("failed to parse Uuid for topic_id: {}", e))?;

        let partitions = buf
            .try_get_i32()
            .map_err(|e| anyhow::anyhow!("failed to parse i32 for partitions: {}", e))?;

        Ok(ForgottenTopic {
            topic_id,
            partitions,
        })
    }
}
