use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
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
                    let server = self.clone();
                    tokio::spawn(async move {
                        server.handle_client(stream).await;
                    });
                }
                Err(e) => {
                    eprintln!("failed to accept connection: {}", e);
                }
            }
        }
    }

    async fn handle_client(&self, mut stream: TcpStream) {
        loop {
            let request = match self.read_request(&mut stream).await {
                Ok(req) => req,
                Err(e) => {
                    eprintln!("{}", e);
                    return;
                }
            };

            println!("parsed request: {:?}", request);

            let response = self.build_response(&request);

            if let Err(e) = self.write_response(&mut stream, response).await {
                eprintln!("error writing response: {}", e);
            }
        }
    }

    async fn read_request(&self, stream: &mut TcpStream) -> Result<RequestV0> {
        let mut buf = [0; 1024];
        let n = stream
            .read(&mut buf)
            .await
            .map_err(|e| format!("error reading from stream: {}", e))?;
        if n == 0 {
            return Err("connection closed by client".into());
        }
        println!("received {} bytes from client", n);

        let rdr = &mut std::io::Cursor::new(&buf[..n]);
        RequestV0::from_be_bytes(rdr).map_err(|e| format!("failed to parse request: {}", e).into())
    }

    fn build_response(&self, request: &RequestV0) -> ResponseV0 {
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

        let response_header = ResponseHeaderV2::new(request.header().correlation_id());
        ResponseV0::new(
            response_body.to_be_bytes().len() as i32 + response_header.to_be_bytes().len() as i32,
            response_header,
            ResponseBody::ApiVersionsResponseV4(response_body),
        )
    }

    async fn write_response(
        &self,
        stream: &mut TcpStream,
        response: ResponseV0,
    ) -> std::io::Result<()> {
        stream.write_all(&response.to_be_bytes()).await?;
        stream.flush().await
    }
}
