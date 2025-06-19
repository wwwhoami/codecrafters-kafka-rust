use std::net::SocketAddr;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use uuid::Uuid;

use crate::protocol::{
    bytes::{FromBytes, ToBytes},
    primitives::{ApiKey, CompactArray, CompactString},
    request::RequestV0,
    response::{
        ApiVersion, ApiVersionsResponseBodyV4, DescribeTopicPartiotionsResponseBodyV0, ErrorCode,
        ResponseBody, ResponseHeader, ResponseHeaderV0, ResponseHeaderV1, ResponseV0, Topic,
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
        let mut buf = [0; 1024];
        let n = self.stream.read(&mut buf).await?;
        if n == 0 {
            return Err(("connection closed").into());
        }

        println!("client {}: received {} bytes", self.peer_addr, n);

        let rdr = &mut std::io::Cursor::new(&buf[..n]);

        RequestV0::from_be_bytes(rdr)
    }

    fn build_response(&self, request: &RequestV0) -> ResponseV0 {
        let response_body = match request.header().request_api_key() {
            ApiKey::ApiVersions => match request.header().request_api_version() {
                0..=4 => ResponseBody::ApiVersionsResponseV4(ApiVersionsResponseBodyV4::new(
                    ErrorCode::None,
                    CompactArray::new(vec![
                        ApiVersion::new(ApiKey::ApiVersions, 0, 4, CompactArray::new(vec![])),
                        ApiVersion::new(
                            ApiKey::DescribeTopicPartitions,
                            0,
                            0,
                            CompactArray::new(vec![]),
                        ),
                    ]),
                    0,
                    CompactArray::new(vec![]),
                )),
                _ => ResponseBody::ApiVersionsResponseV4(ApiVersionsResponseBodyV4::new(
                    ErrorCode::UnsupportedVersion,
                    CompactArray::new(vec![]),
                    0,
                    CompactArray::new(vec![]),
                )),
            },
            ApiKey::DescribeTopicPartitions => {
                let topic_name = request
                    .body()
                    .as_describe_topic_partitions_request_v0()
                    .unwrap()
                    .topics()
                    .to_vec()
                    .first()
                    .unwrap()
                    .topic()
                    .to_string();

                ResponseBody::DescribeTopicPartiotionsResponseV0(
                    DescribeTopicPartiotionsResponseBodyV0::new(
                        0,
                        CompactArray::new(vec![Topic::new(
                            ErrorCode::UnknownTopicOrPartition,
                            CompactString::from_str(topic_name.as_str()),
                            Uuid::nil(),
                            false,
                            CompactArray::new(vec![]),
                            [0, 0, 0, 0],
                            CompactArray::new(vec![]),
                        )]),
                        u8::MAX,
                        CompactArray::new(vec![]),
                    ),
                )
            }
        };

        let response_header = match request.header().request_api_key() {
            ApiKey::ApiVersions => {
                ResponseHeader::V0(ResponseHeaderV0::new(request.header().correlation_id()))
            }
            ApiKey::DescribeTopicPartitions => {
                ResponseHeader::V1(ResponseHeaderV1::new(request.header().correlation_id()))
            }
        };

        ResponseV0::new(
            response_body.to_be_bytes().len() as i32 + response_header.to_be_bytes().len() as i32,
            response_header,
            response_body,
        )
    }
}
