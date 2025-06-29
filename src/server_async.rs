use std::{
    fs::File,
    io::{BufReader, Read},
    net::SocketAddr,
    sync::Arc,
};

use bytes::{Bytes, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use uuid::Uuid;

use crate::protocol::{
    bytes::{FromBytes, ToBytes},
    cluster_metadata::ClusterMetadata,
    primitives::{ApiKey, CompactArray, CompactString},
    request::{DescribeTopicPartitionsRequestV0, FetchRequestV16, RequestV0, TopicsPartitions},
    response::{
        ApiVersion, ApiVersionsResponseBodyV4, DescribeTopicPartiotionsResponseBodyV0, ErrorCode,
        FetchResponseBodyV16, Partition, ResponseBody, ResponseHeader, ResponseHeaderV0,
        ResponseHeaderV1, ResponseV0, Topic,
    },
};

use crate::Result;

#[derive(Debug, Clone)]
pub struct ServerAsync {
    address: String,
    metadata: Arc<ClusterMetadata>,
}

impl ServerAsync {
    pub fn new(address: &str) -> Result<Self> {
        let metadata =
            File::open("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log")
                .map_err(|e| anyhow::anyhow!("failed to read cluster metadata {}", e))
                .and_then(|file| {
                    ClusterMetadata::try_from(file)
                        .map_err(|e| anyhow::anyhow!("failed to parse cluster metadata: {}", e))
                });

        match metadata {
            Ok(metadata) => Ok(ServerAsync {
                address: address.to_string(),
                metadata: Arc::new(metadata),
            }),
            Err(e) => Err(anyhow::anyhow!("failed to initialize server: {}", e).into()),
        }
    }

    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.address)
            .await
            .map_err(|e| format!("failed to bind to address {}: {}", self.address, e))?;

        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let conn = Connection::new(stream, Arc::clone(&self.metadata)).await?;

                    tokio::spawn(async move {
                        conn.handle().await;
                    });
                }
                Err(e) => {
                    eprintln!("failed to accept connection: {}", e);
                }
            }
        }
    }
}

struct Connection {
    stream: TcpStream,
    peer_addr: SocketAddr,
    metadata: Arc<ClusterMetadata>,
}

impl Connection {
    async fn new(stream: TcpStream, metadata: Arc<ClusterMetadata>) -> Result<Self> {
        let peer_addr = stream.peer_addr()?;

        Ok(Connection {
            stream,
            peer_addr,
            metadata,
        })
    }

    async fn write_response(&mut self, response: ResponseV0) -> std::io::Result<()> {
        self.stream.write_all(&response.to_be_bytes()).await?;
        self.stream.flush().await?;

        println!("client {}: sent response: {:?}", self.peer_addr, response);

        Ok(())
    }

    async fn handle(mut self) {
        loop {
            let request = match self.read_request().await {
                Ok(req) => req,
                Err(e) => {
                    eprintln!("client {}: error reading request: {}", self.peer_addr, e);
                    return;
                }
            };

            println!("client {}: parsed request: {:?}", self.peer_addr, request);

            let response = self.build_response(&request);

            if let Err(e) = self.write_response(response).await {
                eprintln!("error writing response to client {}: {}", self.peer_addr, e);
                return;
            }
        }
    }

    async fn read_request(&mut self) -> Result<RequestV0> {
        let mut buf = BytesMut::with_capacity(1024);
        let n = self.stream.read_buf(&mut buf).await?;
        if n == 0 {
            return Err(("connection closed").into());
        }

        println!("client {}: received {} bytes", self.peer_addr, n);

        RequestV0::from_be_bytes(&mut buf)
    }

    fn build_response(&self, request: &RequestV0) -> ResponseV0 {
        let response_header = Self::build_response_header(request);
        let response_body = self.build_response_body(request);
        let message_size =
            response_body.to_be_bytes().len() as i32 + response_header.to_be_bytes().len() as i32;

        ResponseV0::new(message_size, response_header, response_body)
    }

    fn build_response_header(request: &RequestV0) -> ResponseHeader {
        match request.header().request_api_key() {
            ApiKey::ApiVersions => {
                ResponseHeader::V0(ResponseHeaderV0::new(request.header().correlation_id()))
            }
            ApiKey::DescribeTopicPartitions => {
                ResponseHeader::V1(ResponseHeaderV1::new(request.header().correlation_id()))
            }
            ApiKey::Fetch => {
                ResponseHeader::V1(ResponseHeaderV1::new(request.header().correlation_id()))
            }
        }
    }

    fn build_response_body(&self, request: &RequestV0) -> ResponseBody {
        match request.header().request_api_key() {
            ApiKey::ApiVersions => Self::build_api_versions_response(request),
            ApiKey::DescribeTopicPartitions => {
                self.build_describe_topic_partitions_response(request)
            }
            ApiKey::Fetch => self.build_fetch_response(request),
        }
    }

    fn build_api_versions_response(request: &RequestV0) -> ResponseBody {
        let version = request.header().request_api_version();
        if (0..=4).contains(&version) {
            ResponseBody::ApiVersionsResponseV4(ApiVersionsResponseBodyV4::new(
                ErrorCode::None,
                CompactArray::from_vec(vec![
                    ApiVersion::new(ApiKey::ApiVersions, 0, 4, CompactArray::new()),
                    ApiVersion::new(ApiKey::DescribeTopicPartitions, 0, 0, CompactArray::new()),
                    ApiVersion::new(ApiKey::Fetch, 4, 16, CompactArray::new()),
                ]),
                0,
                CompactArray::new(),
            ))
        } else {
            ResponseBody::ApiVersionsResponseV4(ApiVersionsResponseBodyV4::new(
                ErrorCode::UnsupportedVersion,
                CompactArray::new(),
                0,
                CompactArray::new(),
            ))
        }
    }

    fn build_describe_topic_partitions_response(&self, request: &RequestV0) -> ResponseBody {
        let topic_names = request
            .body()
            .as_describe_topic_partitions_request_v0()
            .unwrap_or(&DescribeTopicPartitionsRequestV0::default())
            .topic_names();

        let topics = topic_names
            .into_iter()
            .map(|topic_name| {
                let topic_records = self.metadata.find_topic_records_by_topic(&topic_name);

                if let Some(record) = topic_records.first() {
                    let topic_uuid = record
                        .record_value()
                        .value()
                        .as_topic_record()
                        .unwrap()
                        .topic_uuid();

                    let partition_records = self
                        .metadata
                        .find_partition_records_by_topic_uuid(topic_uuid);

                    let partitions = CompactArray::from_vec(
                        partition_records
                            .into_iter()
                            .map(Partition::from)
                            .collect::<Vec<Partition>>(),
                    );

                    Topic::new(
                        ErrorCode::None,
                        CompactString::from_str(&topic_name),
                        topic_uuid,
                        false,
                        partitions,
                        0,
                        CompactArray::new(),
                    )
                } else {
                    Topic::from_unknown_topic(&topic_name)
                }
            })
            .collect::<Vec<Topic>>();

        ResponseBody::DescribeTopicPartiotionsResponseV0(
            DescribeTopicPartiotionsResponseBodyV0::new(
                0,
                CompactArray::from_vec(topics),
                u8::MAX,
                CompactArray::new(),
            ),
        )
    }

    fn build_unknown_topic_response(topic_name: &str) -> ResponseBody {
        ResponseBody::DescribeTopicPartiotionsResponseV0(
            DescribeTopicPartiotionsResponseBodyV0::new(
                0,
                CompactArray::from_vec(vec![Topic::new(
                    ErrorCode::UnknownTopicOrPartition,
                    CompactString::from_str(topic_name),
                    Uuid::nil(),
                    false,
                    CompactArray::new(),
                    0,
                    CompactArray::new(),
                )]),
                u8::MAX,
                CompactArray::new(),
            ),
        )
    }

    fn build_fetch_response(&self, request: &RequestV0) -> ResponseBody {
        let topic_id = request
            .body()
            .as_fetch_request_v16()
            .unwrap_or(&FetchRequestV16::default())
            .topics()
            .to_vec()
            .first()
            .unwrap_or(&TopicsPartitions::default())
            .topic_id();

        if topic_id.is_nil() {
            return ResponseBody::FetchResponseV16(FetchResponseBodyV16::default());
        }

        let topic_records = self.metadata.find_topic_records_by_id(&topic_id);

        if topic_records.is_empty() {
            println!("No topic records found for topic ID: {}", topic_id);
            return ResponseBody::FetchResponseV16(FetchResponseBodyV16::unknown_topic(topic_id));
        }

        let topic_name = topic_records
            .first()
            .expect("topic records should not be empty")
            .record_value()
            .value()
            .as_topic_record()
            .expect("record value should be a topic record")
            .name();

        if topic_name.is_empty() {
            println!("Topic name is empty for topic ID: {}", topic_id);

            return ResponseBody::FetchResponseV16(FetchResponseBodyV16::empty_topic(topic_id));
        }

        let partition_ids = self
            .metadata
            .find_partition_record_ids_by_topic_uuid(topic_id);

        match &partition_ids.first() {
            Some(partition_id) => {
                let filename = format!(
                    "/tmp/kraft-combined-logs/{}-{}/00000000000000000000.log",
                    topic_name, partition_id
                );
                let file = File::open(filename);
                match file {
                    Err(e) => {
                        eprintln!(
                            "Failed to open file for topic: {}, partition: {}, error: {}",
                            topic_name, partition_id, e
                        );
                        ResponseBody::FetchResponseV16(FetchResponseBodyV16::empty_topic(topic_id))
                    }
                    Ok(file) => {
                        let mut reader = BufReader::new(file);
                        let mut buf = Vec::new();
                        reader.read_to_end(&mut buf).unwrap();
                        let bytes = Bytes::from(buf);

                        ResponseBody::FetchResponseV16(FetchResponseBodyV16::with_record_for_topic(
                            topic_id,
                            bytes.into(),
                        ))
                    }
                }
            }
            _ => ResponseBody::FetchResponseV16(FetchResponseBodyV16::empty_topic(topic_id)),
        }
    }
}
