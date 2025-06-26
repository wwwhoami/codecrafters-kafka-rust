use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
};

use crate::protocol::{
    bytes::{FromBytes, ToBytes},
    primitives::{ApiKey, CompactArray},
    request::RequestV0,
    response::{
        ApiVersion, ApiVersionsResponseBodyV4, ErrorCode, ResponseBody, ResponseHeader,
        ResponseHeaderV1, ResponseV0,
    },
};

use crate::Result;

#[derive(Debug, Clone)]
pub struct ServerSync {
    address: String,
}

impl ServerSync {
    pub fn new(address: &str) -> Self {
        Self {
            address: address.to_string(),
        }
    }

    pub fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.address)
            .map_err(|e| format!("failed to bind to address {}: {}", self.address, e))?;

        loop {
            match listener.accept() {
                Ok((stream, _)) => {
                    let conn = Connection::new(stream)?;
                    conn.handle();
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
    fn new(stream: TcpStream) -> Result<Self> {
        let peer_addr = stream.peer_addr()?;
        Ok(Connection { stream, peer_addr })
    }

    fn write_response(&mut self, response: ResponseV0) -> std::io::Result<()> {
        self.stream.write_all(&response.to_be_bytes())?;
        self.stream.flush()
    }

    fn handle(mut self) {
        loop {
            let request = match self.read_request() {
                Ok(req) => req,
                Err(e) => {
                    eprintln!("client {}: error reading request: {}", self.peer_addr, e);
                    return;
                }
            };

            println!("client {}: parsed request: {:?}", self.peer_addr, request);

            let response = self.build_response(&request);

            if let Err(e) = self.write_response(response) {
                eprintln!("error writing response to client {}: {}", self.peer_addr, e);
                return;
            }
        }
    }

    fn read_request(&mut self) -> Result<RequestV0> {
        let mut buf = [0; 1024];
        let n = self.stream.read(&mut buf)?;
        if n == 0 {
            return Err(("connection closed").into());
        }

        println!("client {}: received {} bytes", self.peer_addr, n);

        let rdr = &mut std::io::Cursor::new(&buf[..n]);

        RequestV0::from_be_bytes(rdr)
    }

    fn build_response(&self, request: &RequestV0) -> ResponseV0 {
        let response_body = match request.header().request_api_version() {
            0..=4 => ApiVersionsResponseBodyV4::new(
                ErrorCode::None,
                CompactArray::from_vec(vec![
                    ApiVersion::new(ApiKey::ApiVersions, 0, 4, CompactArray::new()),
                    ApiVersion::new(ApiKey::DescribeTopicPartitions, 0, 0, CompactArray::new()),
                ]),
                0,
                CompactArray::new(),
            ),
            _ => ApiVersionsResponseBodyV4::new(
                ErrorCode::UnsupportedVersion,
                CompactArray::new(),
                0,
                CompactArray::new(),
            ),
        };

        let response_header = ResponseHeaderV1::new(request.header().correlation_id());
        ResponseV0::new(
            response_body.to_be_bytes().len() as i32 + response_header.to_be_bytes().len() as i32,
            ResponseHeader::V1(response_header),
            ResponseBody::ApiVersionsResponseV4(response_body),
        )
    }
}
