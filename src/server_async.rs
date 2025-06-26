use std::{fs::File, net::SocketAddr};

use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use uuid::Uuid;

use crate::protocol::{
    bytes::{FromBytes, ToBytes},
    cluster_metadata::{self, ClusterMetadata},
    primitives::{ApiKey, CompactArray, CompactString, VarInt},
    request::RequestV0,
    response::{
        ApiVersion, ApiVersionsResponseBodyV4, DescribeTopicPartiotionsResponseBodyV0, ErrorCode,
        Partition, ResponseBody, ResponseHeader, ResponseHeaderV0, ResponseHeaderV1, ResponseV0,
        Topic,
    },
};

use crate::Result;

#[derive(Debug, Clone)]
pub struct ServerAsync {
    address: String,
}

impl ServerAsync {
    pub fn new(address: &str) -> Self {
        Self {
            address: address.to_string(),
        }
    }

    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.address)
            .await
            .map_err(|e| format!("failed to bind to address {}: {}", self.address, e))?;

        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let conn = Connection::new(stream).await?;

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
}

impl Connection {
    async fn new(stream: TcpStream) -> Result<Self> {
        let peer_addr = stream.peer_addr()?;
        Ok(Connection { stream, peer_addr })
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
        let response_body = Self::build_response_body(request);

        ResponseV0::new(
            response_body.to_be_bytes().len() as i32 + response_header.to_be_bytes().len() as i32,
            response_header,
            response_body,
        )
    }

    fn build_response_header(request: &RequestV0) -> ResponseHeader {
        match request.header().request_api_key() {
            ApiKey::ApiVersions => {
                ResponseHeader::V0(ResponseHeaderV0::new(request.header().correlation_id()))
            }
            ApiKey::DescribeTopicPartitions => {
                ResponseHeader::V1(ResponseHeaderV1::new(request.header().correlation_id()))
            }
        }
    }

    fn build_response_body(request: &RequestV0) -> ResponseBody {
        match request.header().request_api_key() {
            ApiKey::ApiVersions => Self::build_api_versions_response(request),
            ApiKey::DescribeTopicPartitions => {
                Self::build_describe_topic_partitions_response(request)
            }
        }
    }

    fn build_api_versions_response(request: &RequestV0) -> ResponseBody {
        let version = request.header().request_api_version();
        if (0..=4).contains(&version) {
            ResponseBody::ApiVersionsResponseV4(ApiVersionsResponseBodyV4::new(
                ErrorCode::None,
                CompactArray::new(vec![
                    ApiVersion::new(ApiKey::ApiVersions, 0, 4, CompactArray::default()),
                    ApiVersion::new(
                        ApiKey::DescribeTopicPartitions,
                        0,
                        0,
                        CompactArray::default(),
                    ),
                ]),
                0,
                CompactArray::default(),
            ))
        } else {
            ResponseBody::ApiVersionsResponseV4(ApiVersionsResponseBodyV4::new(
                ErrorCode::UnsupportedVersion,
                CompactArray::default(),
                0,
                CompactArray::default(),
            ))
        }
    }

    fn build_describe_topic_partitions_response(request: &RequestV0) -> ResponseBody {
        let topic_name = request
            .body()
            .as_describe_topic_partitions_request_v0()
            .and_then(|b| b.topics().to_vec().first().map(|t| t.topic().to_string()))
            .unwrap_or_else(|| {
                eprintln!("no topic name provided in DescribeTopicPartitionsRequestV0");
                "unknown".to_string()
            });

        let metadata =
            File::open("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log")
                .map_err(|e| anyhow::anyhow!("failed to read cluster metadata {}", e))
                .and_then(|file| {
                    ClusterMetadata::try_from(file)
                        .map_err(|e| anyhow::anyhow!("failed to parse cluster metadata: {}", e))
                });

        match metadata {
            Ok(metadata) => {
                let records = metadata.find_topic_records_by_topic(&topic_name);

                if let Some(record) = records.first() {
                    let topic_uuid = record
                        .record_value()
                        .value()
                        .as_topic_record()
                        .unwrap()
                        .topic_uuid();
                    ResponseBody::DescribeTopicPartiotionsResponseV0(
                        DescribeTopicPartiotionsResponseBodyV0::new(
                            0,
                            CompactArray::new(vec![Topic::new(
                                ErrorCode::None,
                                CompactString::from_str(&topic_name),
                                topic_uuid,
                                false,
                                CompactArray::new(vec![Partition::new(
                                    ErrorCode::None,
                                    0,
                                    0,
                                    0,
                                    CompactArray::default(),
                                    CompactArray::default(),
                                    VarInt::from(0),
                                    0,
                                    0,
                                    0,
                                )]),
                                0,
                                CompactArray::default(),
                            )]),
                            u8::MAX,
                            CompactArray::default(),
                        ),
                    )
                } else {
                    Self::build_unknown_topic_response(&topic_name)
                }
            }
            Err(e) => {
                println!("error reading cluster metadata: {}", e);
                Self::build_unknown_topic_response(&topic_name)
            }
        }
    }

    fn build_unknown_topic_response(topic_name: &str) -> ResponseBody {
        ResponseBody::DescribeTopicPartiotionsResponseV0(
            DescribeTopicPartiotionsResponseBodyV0::new(
                0,
                CompactArray::new(vec![Topic::new(
                    ErrorCode::UnknownTopicOrPartition,
                    CompactString::from_str(topic_name),
                    Uuid::nil(),
                    false,
                    CompactArray::default(),
                    0,
                    CompactArray::default(),
                )]),
                u8::MAX,
                CompactArray::default(),
            ),
        )
    }
}
