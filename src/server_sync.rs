use std::{
    io::{Cursor, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
};

use crate::protocol::{
    bytes::{FromBytes, ToBytes},
    primitives::CompactArray,
    request::RequestV0,
    response::{
        ApiVersion, ApiVersionsResponseBodyV4, ErrorCode, ResponseBody, ResponseHeaderV2,
        ResponseV0,
    },
};

use crate::Result;

pub struct ServerSync {
    address: SocketAddr,
}

impl ServerSync {
    pub fn new(address: &str) -> Result<Self> {
        let socket_addr = std::net::SocketAddr::V4(address.parse()?);

        Ok(Self {
            address: socket_addr,
        })
    }

    pub fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(self.address)
            .map_err(|e| format!("failed to bind to address {}: {}", self.address, e))?;

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    self.handle_client(stream);
                }
                Err(e) => {
                    eprintln!("error: {}", e);
                }
            }
        }
        Ok(())
    }

    fn handle_client(&self, mut stream: TcpStream) {
        loop {
            let request = match self.read_request(&mut stream) {
                Ok(req) => req,
                Err(e) => {
                    eprintln!("{}", e);
                    break;
                }
            };

            println!("parsed request: {:?}", request);

            let response = self.build_response(&request);

            if let Err(e) = self.write_response(&mut stream, response) {
                eprintln!("error writing response: {}", e);
                break;
            }
        }
    }

    fn read_request(&self, stream: &mut TcpStream) -> Result<RequestV0> {
        let mut buf = [0; 1024];
        let n = stream
            .read(&mut buf)
            .map_err(|e| format!("error reading from stream: {}", e))?;

        if n == 0 {
            return Err("connection closed by client".into());
        }

        println!("received {} bytes from client", n);

        let rdr = &mut Cursor::new(&buf[..n]);
        RequestV0::from_be_bytes(rdr).map_err(|e| format!("error parsing request: {}", e).into())
    }

    fn build_response(&self, request: &RequestV0) -> ResponseV0 {
        let response_header = ResponseHeaderV2::new(request.header().correlation_id());
        let response_body = match request.header().request_api_version() {
            0..=4 => ApiVersionsResponseBodyV4::new(
                ErrorCode::None,
                CompactArray::new(vec![ApiVersion::new(18, 0, 4, CompactArray::new(vec![]))]),
                0,
                CompactArray::new(vec![]),
            ),
            _ => ApiVersionsResponseBodyV4::new(
                ErrorCode::UnsupportedVersion,
                CompactArray::new(vec![]),
                0,
                CompactArray::new(vec![]),
            ),
        };
        let message_size =
            response_body.to_be_bytes().len() as i32 + response_header.to_be_bytes().len() as i32;

        ResponseV0::new(
            message_size,
            response_header,
            ResponseBody::ApiVersionsResponseV4(response_body),
        )
    }

    fn write_response(&self, stream: &mut TcpStream, response: ResponseV0) -> std::io::Result<()> {
        println!("sending response: {:?}", response);

        stream.write_all(&response.to_be_bytes())?;
        stream.flush()
    }
}
