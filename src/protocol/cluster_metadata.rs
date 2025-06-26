use bytes::{Buf, Bytes};

use super::{
    bytes::FromBytes,
    primitives::{CompactArray, CompactString, VarInt, INT32},
};

use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufReader, Read},
};

use crate::{protocol::primitives::UnsignedVarInt, Result};

#[derive(Debug)]
pub(crate) struct ClusterMetadata {
    batches: BTreeMap<i64, Batch>,
}

impl ClusterMetadata {
    fn find_batches_by_topic(&self, topic: &str) -> Vec<&Batch> {
        self.batches
            .iter()
            .find(|(_, batch)| !batch.find_topic_records_by_topic(topic).is_empty())
            .into_iter()
            .map(|(_, batch)| batch)
            .collect()
    }
}

impl ClusterMetadata {
    pub fn values(&self) -> std::collections::btree_map::Values<'_, i64, Batch> {
        self.batches.values()
    }

    pub fn find_topic_records_by_topic(&self, topic: &str) -> Vec<&Record> {
        self.batches
            .values()
            .flat_map(|batch| batch.find_topic_records_by_topic(topic))
            .collect()
    }

    pub fn find_partition_records_by_topic_uuid(
        &self,
        topic_uuid: uuid::Uuid,
    ) -> Vec<&PartitionRecordValue> {
        self.batches
            .values()
            .flat_map(|batch| batch.find_partition_records_by_topic_uuid(topic_uuid))
            .collect()
    }
}

impl TryFrom<File> for ClusterMetadata {
    type Error = std::io::Error;

    fn try_from(file: File) -> std::result::Result<Self, Self::Error> {
        let mut reader = BufReader::new(file);
        let mut batches = BTreeMap::new();

        let mut vec = Vec::new();
        reader.read_to_end(&mut vec).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("failed to read file: {}", e),
            )
        })?;
        let mut bytes = Bytes::from(vec);

        while let Ok(base_offset) = bytes.try_get_i64() {
            let batch_length = bytes.try_get_i32()?;

            let mut contents = bytes.split_to(batch_length as usize);

            batches.insert(
                base_offset,
                Batch::try_from(&mut contents).map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("failed to parse batch from contents: {}", e),
                    )
                })?,
            );
        }

        Ok(ClusterMetadata { batches })
    }
}

#[derive(Debug)]
pub(crate) struct Batch {
    partition_leader_epoch: i32,
    magic_byte: u8,
    crc: i32,
    attributes: u16,
    last_offset_delta: i32,
    base_timestamp: i64,
    max_timestamp: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    records: Vec<Record>,
}

impl Batch {
    fn find_topic_records_by_topic(&self, topic: &str) -> Vec<&Record> {
        self.records
            .iter()
            .filter(|record| {
                if let RecordValueByType::Topic(topic_value) = &record.record_value.value {
                    topic_value.name == topic
                } else {
                    false
                }
            })
            .collect()
    }

    fn find_partition_records_by_topic_uuid(
        &self,
        topic_uuid: uuid::Uuid,
    ) -> Vec<&PartitionRecordValue> {
        self.records
            .iter()
            .filter_map(|record| {
                if let RecordValueByType::Partition(partition_value) = &record.record_value.value {
                    if partition_value.topic_uuid == topic_uuid {
                        Some(partition_value)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect()
    }
}

impl TryFrom<&mut bytes::Bytes> for Batch {
    type Error = std::io::Error;

    fn try_from(mut bytes: &mut bytes::Bytes) -> std::result::Result<Self, Self::Error> {
        let partition_leader_epoch = bytes.try_get_i32()?;
        let magic_byte = bytes.try_get_u8()?;
        let crc = bytes.try_get_i32()?;
        let attributes = bytes.try_get_u16()?;
        let last_offset_delta = bytes.try_get_i32()?;
        let base_timestamp = bytes.try_get_i64()?;
        let max_timestamp = bytes.try_get_i64()?;
        let producer_id = bytes.try_get_i64()?;
        let producer_epoch = bytes.try_get_i16()?;
        let base_sequence = bytes.try_get_i32()?;

        let records_length = bytes.try_get_i32()?;
        let mut records = Vec::with_capacity(records_length as usize);

        for _ in 0..records_length {
            let record = Record::try_from(&mut *bytes).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("failed to parse record from batch: {}", e),
                )
            })?;

            records.push(record);
        }

        Ok(Batch {
            partition_leader_epoch,
            magic_byte,
            crc,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        })
    }
}

#[derive(Debug)]
pub(crate) struct Record {
    record_length: VarInt,
    attributes: u8,
    timestamp_delta: VarInt,
    offset_delta: VarInt,
    key: Vec<u8>,
    record_value: RecordValue,
    headers_array_count: u32,
}

impl Record {
    pub(crate) fn record_value(&self) -> &RecordValue {
        &self.record_value
    }
}

impl TryFrom<&mut bytes::Bytes> for Record {
    type Error = crate::Error;

    fn try_from(mut bytes: &mut bytes::Bytes) -> std::result::Result<Self, Self::Error> {
        let record_length = VarInt::from_be_bytes(&mut bytes)?;
        let attributes = bytes.try_get_u8()?;
        let timestamp_delta = VarInt::from_be_bytes(&mut bytes)?;
        let offset_delta = VarInt::from_be_bytes(&mut bytes)?;
        let key_length = VarInt::from_be_bytes(&mut bytes)?.value();
        let key = if key_length < 0 {
            Vec::new()
        } else {
            bytes.split_to(key_length as usize).to_vec()
        };

        let value_length = VarInt::from_be_bytes(&mut bytes)?.value();
        let mut record_contents = if value_length < 0 {
            Bytes::new()
        } else {
            bytes.split_to(value_length as usize)
        };

        let record_value = RecordValue::try_from(&mut record_contents).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("failed to parse record value: {}", e),
            )
        })?;

        let headers_array_count = UnsignedVarInt::from_be_bytes(&mut bytes)?.value();

        Ok(Record {
            record_length,
            attributes,
            timestamp_delta,
            offset_delta,
            key,
            record_value,
            headers_array_count,
        })
    }
}

#[derive(Debug)]
pub(crate) struct RecordValue {
    frame_version: i8,
    record_type: i8,
    version: i8,
    value: RecordValueByType,
}

impl RecordValue {
    pub(crate) fn value(&self) -> &RecordValueByType {
        &self.value
    }
}

impl TryFrom<&mut bytes::Bytes> for RecordValue {
    type Error = crate::Error;

    fn try_from(bytes: &mut bytes::Bytes) -> std::result::Result<Self, Self::Error> {
        let frame_version = bytes.try_get_i8()?;
        let record_type = bytes.try_get_i8()?;
        let version = bytes.try_get_i8()?;

        let value = RecordValueByType::try_from(bytes, record_type).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("failed to parse record value: {}", e),
            )
        })?;

        Ok(RecordValue {
            frame_version,
            record_type,
            version,
            value,
        })
    }
}

#[derive(Debug)]
pub(crate) enum RecordValueByType {
    Feature(FeatureRecordValue),
    Topic(TopicRecordValue),
    Partition(PartitionRecordValue),
}

impl RecordValueByType {
    fn try_from(bytes: &mut bytes::Bytes, record_type: i8) -> Result<Self> {
        match record_type {
            12 => Ok(Self::Feature(FeatureRecordValue::try_from(bytes)?)),
            2 => Ok(Self::Topic(TopicRecordValue::try_from(bytes)?)),
            3 => Ok(Self::Partition(PartitionRecordValue::try_from(bytes)?)),
            _ => Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unknown record type: {}", record_type),
            ))),
        }
    }

    pub(crate) fn as_feature(&self) -> Option<&FeatureRecordValue> {
        if let Self::Feature(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub(crate) fn as_topic_record(&self) -> Option<&TopicRecordValue> {
        if let Self::Topic(topic_value) = self {
            Some(topic_value)
        } else {
            None
        }
    }

    pub(crate) fn as_partition(&self) -> Option<&PartitionRecordValue> {
        if let Self::Partition(v) = self {
            Some(v)
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct FeatureRecordValue {
    name: String,
    feature_level: i16,
    tagged_fields_count: u32,
}

impl TryFrom<&mut bytes::Bytes> for FeatureRecordValue {
    type Error = crate::Error;

    fn try_from(mut bytes: &mut bytes::Bytes) -> std::result::Result<Self, Self::Error> {
        let name = CompactString::from_be_bytes(&mut bytes)
            .map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid feature name: {}", e),
                )
            })?
            .to_string();

        let feature_level = bytes.try_get_i16()?;
        let tagged_fields_count = UnsignedVarInt::from_be_bytes(&mut bytes)?.value();

        Ok(Self {
            name,
            feature_level,
            tagged_fields_count,
        })
    }
}

#[derive(Debug)]
pub struct TopicRecordValue {
    name: String,
    topic_uuid: uuid::Uuid,
    tagged_fields_count: u32,
}

impl TopicRecordValue {
    pub fn topic_uuid(&self) -> uuid::Uuid {
        self.topic_uuid
    }
}

impl TryFrom<&mut bytes::Bytes> for TopicRecordValue {
    type Error = crate::Error;

    fn try_from(mut bytes: &mut bytes::Bytes) -> std::result::Result<Self, Self::Error> {
        let name = CompactString::from_be_bytes(&mut bytes)
            .map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid topic name: {}", e),
                )
            })?
            .to_string();

        let topic_uuid = uuid::Uuid::from_slice(&bytes.split_to(16)).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid UUID in topic record",
            )
        })?;
        let tagged_fields_count = UnsignedVarInt::from_be_bytes(&mut bytes)?.value();

        Ok(Self {
            name,
            topic_uuid,
            tagged_fields_count,
        })
    }
}

#[derive(Debug)]
pub struct PartitionRecordValue {
    partition_id: i32,
    topic_uuid: uuid::Uuid,
    replica_array: CompactArray<INT32>,
    in_sync_replica_array: CompactArray<INT32>,
    removing_replicas_array: CompactArray<INT32>,
    adding_replicas_array: CompactArray<INT32>,
    leader: i32,
    leader_epoch: i32,
    partition_epoch: i32,
    directories_array: Vec<uuid::Uuid>,
    tagged_fields_count: u32,
}

impl PartitionRecordValue {
    pub fn partition_id(&self) -> i32 {
        self.partition_id
    }

    pub fn topic_uuid(&self) -> uuid::Uuid {
        self.topic_uuid
    }

    pub fn replica_array(&self) -> &CompactArray<INT32> {
        &self.replica_array
    }

    pub fn in_sync_replica_array(&self) -> &CompactArray<INT32> {
        &self.in_sync_replica_array
    }

    pub fn removing_replicas_array(&self) -> &CompactArray<INT32> {
        &self.removing_replicas_array
    }

    pub fn adding_replicas_array(&self) -> &CompactArray<INT32> {
        &self.adding_replicas_array
    }

    pub fn leader(&self) -> i32 {
        self.leader
    }

    pub fn leader_epoch(&self) -> i32 {
        self.leader_epoch
    }

    pub fn partition_epoch(&self) -> i32 {
        self.partition_epoch
    }

    pub fn directories_array(&self) -> &[uuid::Uuid] {
        &self.directories_array
    }

    pub fn tagged_fields_count(&self) -> u32 {
        self.tagged_fields_count
    }
}

impl TryFrom<&mut bytes::Bytes> for PartitionRecordValue {
    type Error = crate::Error;

    fn try_from(mut bytes: &mut bytes::Bytes) -> std::result::Result<Self, Self::Error> {
        let partition_id = bytes.try_get_i32()?;
        let topic_uuid = uuid::Uuid::from_slice(&bytes.split_to(16)).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid UUID in partition record",
            )
        })?;

        let replica_array = CompactArray::from_be_bytes(&mut bytes).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "failed to parse replica array",
            )
        })?;
        let in_sync_replica_array = CompactArray::from_be_bytes(&mut bytes).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "failed to parse in-sync replica array",
            )
        })?;
        let removing_replicas_array = CompactArray::from_be_bytes(&mut bytes).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "failed to parse removing replicas array",
            )
        })?;
        let adding_replicas_array = CompactArray::from_be_bytes(&mut bytes).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "failed to parse adding replicas array",
            )
        })?;

        let leader = bytes.try_get_i32()?;
        let leader_epoch = bytes.try_get_i32()?;
        let partition_epoch = bytes.try_get_i32()?;

        let directories_array_len = UnsignedVarInt::from_be_bytes(&mut bytes)?.value() - 1;
        let mut directories_array = Vec::with_capacity(directories_array_len as usize);
        for _ in 0..directories_array_len {
            directories_array.push(uuid::Uuid::from_slice(&bytes.split_to(16)).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "invalid UUID in partition record",
                )
            })?);
        }

        let tagged_fields_count = UnsignedVarInt::from_be_bytes(&mut bytes)?.value();

        Ok(Self {
            partition_id,
            topic_uuid,
            replica_array,
            in_sync_replica_array,
            removing_replicas_array,
            adding_replicas_array,
            leader,
            leader_epoch,
            partition_epoch,
            directories_array,
            tagged_fields_count,
        })
    }
}
