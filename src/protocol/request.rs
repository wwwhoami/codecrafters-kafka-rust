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
}

impl ToBytes for RequestHeaderV2 {
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.request_api_key.to_be_bytes());
        bytes.extend_from_slice(&self.request_api_version.to_be_bytes());
        bytes.extend_from_slice(&self.correlation_id.to_be_bytes());

        bytes.extend_from_slice(&self.client_id.to_be_bytes());
        bytes.extend_from_slice(&self.tag.to_be_bytes());

        bytes
    }
}
impl FromBytes for RequestHeaderV2 {
    fn from_be_bytes<R: std::io::Read>(mut reader: &mut R) -> Result<Self> {
        let mut buf2 = [0u8; 2];
        let mut buf4 = [0u8; 4];

        let request_api_key = ApiKey::from_be_bytes(&mut reader)
            .map_err(|e| anyhow::anyhow!("failed to parse request_api_key: {}", e))?;

        reader
            .read_exact(&mut buf2)
            .map_err(|e| anyhow::anyhow!("failed to read request_api_version: {}", e))?;
        let request_api_version = i16::from_be_bytes(buf2);

        reader
            .read_exact(&mut buf4)
            .map_err(|e| anyhow::anyhow!("failed to read correlation_id: {}", e))?;
        let correlation_id = i32::from_be_bytes(buf4);

        let client_id = NullableString::from_be_bytes(&mut reader)
            .map_err(|e| anyhow::anyhow!("failed to parse NullableString for client_id: {}", e))?;
        let tag = CompactArray::<NullableString>::from_be_bytes(&mut reader).map_err(|e| {
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
}

impl ToBytes for RequestV0 {
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        bytes.extend_from_slice(&self.message_size.to_be_bytes());
        bytes.extend_from_slice(&self.header.to_be_bytes());

        bytes
    }
}

impl FromBytes for RequestV0 {
    fn from_be_bytes<R: std::io::Read>(mut reader: &mut R) -> Result<Self> {
        let mut buf4 = [0u8; 4];

        reader
            .read_exact(&mut buf4)
            .map_err(|e| anyhow::anyhow!("failed to read message size: {}", e))?;

        let message_size = i32::from_be_bytes(buf4);

        let header = RequestHeaderV2::from_be_bytes(&mut reader)
            .map_err(|e| anyhow::anyhow!("failed to parse RequestHeaderV2: {}", e))?;

        let body = match header.request_api_key {
            ApiKey::ApiVersions => RequestBody::ApiVersionsRequestV4(
                ApiVersionsRequestV4::from_be_bytes(&mut reader)
                    .map_err(|e| anyhow::anyhow!("failed to parse ApiVersionsRequestV4: {}", e))?,
            ),
            ApiKey::DescribeTopicPartitions => {
                // Placeholder for future request body parsing
                return Err(
                    anyhow::anyhow!("DescribeTopicPartitions request not implemented").into(),
                );
            }
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
    fn from_be_bytes<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let client_software_name = CompactString::from_be_bytes(reader).map_err(|e| {
            anyhow::anyhow!(
                "failed to parse CompactString for client_software_name: {}",
                e
            )
        })?;
        let client_software_version = CompactString::from_be_bytes(reader).map_err(|e| {
            anyhow::anyhow!(
                "failed to parse CompactString for client_software_version: {}",
                e
            )
        })?;
        let tag = CompactArray::<NullableString>::from_be_bytes(reader).map_err(|e| {
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
