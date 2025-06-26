use bytes::{BufMut, Bytes, BytesMut};
use uuid::Uuid;

use super::{
    bytes::ToBytes,
    cluster_metadata::PartitionRecordValue,
    primitives::{ApiKey, CompactArray, CompactString, NullableString, VarInt, INT32},
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
    fn to_be_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        buf.put_i32(self.message_size);
        buf.extend_from_slice(&self.header.to_be_bytes());
        buf.extend_from_slice(&self.body.to_be_bytes());

        buf.freeze()
    }
}

#[derive(Debug)]
pub enum ResponseHeader {
    V0(ResponseHeaderV0),
    V1(ResponseHeaderV1),
}

impl ToBytes for ResponseHeader {
    fn to_be_bytes(&self) -> Bytes {
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
    fn to_be_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        buf.put_i32(self.correlation_id);

        buf.freeze()
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
            tag: CompactArray::new(),
        }
    }
}

impl ToBytes for ResponseHeaderV1 {
    fn to_be_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        buf.put_i32(self.correlation_id);
        buf.extend_from_slice(&self.tag.to_be_bytes());

        buf.freeze()
    }
}

#[derive(Debug)]
pub enum ResponseBody {
    ApiVersionsResponseV4(ApiVersionsResponseBodyV4),
    DescribeTopicPartiotionsResponseV0(DescribeTopicPartiotionsResponseBodyV0),
}

impl ToBytes for ResponseBody {
    fn to_be_bytes(&self) -> Bytes {
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
    fn to_be_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        buf.put_i16(self.error_code.clone() as i16);
        buf.extend_from_slice(&self.api_versions.to_be_bytes());
        buf.put_i32(self.throttle_time_ms);
        buf.extend_from_slice(&self.tag.to_be_bytes());

        buf.freeze()
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
    fn to_be_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        buf.extend_from_slice(&self.api_key.to_be_bytes());
        buf.put_i16(self.min_version);
        buf.put_i16(self.max_version);
        buf.extend_from_slice(&self.tag.to_be_bytes());

        buf.freeze()
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
    fn to_be_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        buf.put_i32(self.throttle_time_ms);
        buf.extend_from_slice(&self.topics.to_be_bytes());
        buf.put_u8(self.next_cursor);
        buf.extend_from_slice(&self.tag.to_be_bytes());

        buf.freeze()
    }
}

#[derive(Debug)]
pub struct Topic {
    error_code: ErrorCode,
    name: CompactString,
    id: Uuid,
    is_internal: bool,
    partitions: CompactArray<Partition>,
    authorized_operations: u32,
    tag: CompactArray<NullableString>,
}

impl Topic {
    pub fn new(
        error_code: ErrorCode,
        name: CompactString,
        id: Uuid,
        is_internal: bool,
        partitions: CompactArray<Partition>,
        authorized_operations: u32,
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

    pub fn from_unknown_topic(topic_name: &str) -> Topic {
        Self {
            error_code: ErrorCode::UnknownTopicOrPartition,
            name: CompactString::from_str(topic_name),
            id: Uuid::nil(),
            is_internal: false,
            partitions: CompactArray::new(),
            authorized_operations: 0,
            tag: CompactArray::new(),
        }
    }
}

impl ToBytes for Topic {
    fn to_be_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        buf.put_i16(self.error_code.clone() as i16);
        buf.extend_from_slice(&self.name.to_be_bytes());
        buf.extend_from_slice(self.id.as_bytes());
        buf.put_u8(self.is_internal as u8);
        buf.extend_from_slice(&self.partitions.to_be_bytes());
        buf.put_u32(self.authorized_operations);
        buf.extend_from_slice(&self.tag.to_be_bytes());

        buf.freeze()
    }
}

#[derive(Debug)]
pub(crate) struct Partition {
    error_code: ErrorCode,
    partition_index: i32,
    leader: i32,
    leader_epoch: i32,
    // replica_nodes_len: VarInt, // varint
    replica_nodes: CompactArray<INT32>, // replica nodes
    // isr_nodes_len: VarInt, // varint
    isr_nodes: CompactArray<INT32>,   // in-sync replica nodes
    eligible_leader_replicas: VarInt, //varint 0 for now
    last_known_elr: u8,
    offline_replicas: u8,
    tag_buffer: u8,
}

impl Partition {
    pub(crate) fn new(
        error_code: ErrorCode,
        partition_index: i32,
        leader: i32,
        leader_epoch: i32,
        replica_nodes: CompactArray<INT32>,
        isr_nodes: CompactArray<INT32>,
        eligible_leader_replicas: VarInt,
        last_known_elr: u8,
        offline_replicas: u8,
        tag_buffer: u8,
    ) -> Self {
        Self {
            error_code,
            partition_index,
            leader,
            leader_epoch,
            replica_nodes,
            isr_nodes,
            eligible_leader_replicas,
            last_known_elr,
            offline_replicas,
            tag_buffer,
        }
    }
}

impl ToBytes for Partition {
    fn to_be_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        buf.put_i16(self.error_code.clone() as i16);
        buf.put_i32(self.partition_index);
        buf.put_i32(self.leader);
        buf.put_i32(self.leader_epoch);
        buf.extend_from_slice(&self.replica_nodes.to_be_bytes());
        buf.extend_from_slice(&self.isr_nodes.to_be_bytes());
        buf.extend_from_slice(&self.eligible_leader_replicas.to_be_bytes());
        buf.put_u8(self.last_known_elr);
        buf.put_u8(self.offline_replicas);
        buf.put_u8(self.tag_buffer);

        buf.freeze()
    }
}

impl From<&PartitionRecordValue> for Partition {
    fn from(partition_record: &PartitionRecordValue) -> Self {
        Partition {
            error_code: ErrorCode::None,
            partition_index: partition_record.partition_id(),
            leader: partition_record.leader(),
            leader_epoch: partition_record.leader_epoch(),
            replica_nodes: partition_record.replica_array().clone(),
            isr_nodes: partition_record.in_sync_replica_array().clone(),
            eligible_leader_replicas: VarInt::from(0),
            last_known_elr: 0,
            offline_replicas: 0,
            tag_buffer: 0,
        }
    }
}
