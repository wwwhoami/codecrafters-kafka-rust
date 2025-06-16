use std::{
    io::{Cursor, Read, Write},
    net::TcpListener,
};

use anyhow::Error;

#[derive(Debug)]
struct NullableString {
    value: Option<String>,
}

impl NullableString {
    fn to_be_bytes(&self) -> Vec<u8> {
        if self.value.is_none() {
            (-1_i16).to_be_bytes().to_vec() // -1 for empty string
        } else {
            let value = self.value.as_ref().unwrap();
            let mut bytes = (value.len() as i16).to_be_bytes().to_vec();
            bytes.extend_from_slice(value.as_bytes());
            bytes
        }
    }

    fn from_be_bytes<R: std::io::Read>(reader: &mut R) -> Result<Self, Error> {
        let mut len_buf = [0u8; 2];
        reader.read_exact(&mut len_buf)?;

        let len = i16::from_be_bytes(len_buf);
        if len < 0 {
            Ok(NullableString { value: None })
        } else {
            let mut str_buf = vec![0u8; len as usize];
            reader.read_exact(&mut str_buf)?;
            Ok(NullableString {
                value: Some(String::from_utf8(str_buf)?),
            })
        }
    }
}

#[derive(Debug)]
struct RequestHeaderV2 {
    request_api_key: i16,
    request_api_version: i16,
    correlation_id: i32,
    client_id: NullableString,
    tag: NullableString,
}

impl RequestHeaderV2 {
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.request_api_key.to_be_bytes());
        bytes.extend_from_slice(&self.request_api_version.to_be_bytes());
        bytes.extend_from_slice(&self.correlation_id.to_be_bytes());

        bytes.extend_from_slice(&self.client_id.to_be_bytes());
        bytes.extend_from_slice(&self.tag.to_be_bytes());

        bytes
    }

    fn from_be_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let mut rdr = Cursor::new(bytes);

        let mut buf2 = [0u8; 2];
        let mut buf4 = [0u8; 4];

        rdr.read_exact(&mut buf2)?;
        let request_api_key = i16::from_be_bytes(buf2);

        rdr.read_exact(&mut buf2)?;
        let request_api_version = i16::from_be_bytes(buf2);

        rdr.read_exact(&mut buf4)?;
        let correlation_id = i32::from_be_bytes(buf4);

        let client_id = NullableString::from_be_bytes(&mut rdr)?;
        let tag = NullableString::from_be_bytes(&mut rdr)?;

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
struct RequestV0 {
    message_size: i32,
    header: RequestHeaderV2,
}

impl RequestV0 {
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        bytes.extend_from_slice(&self.message_size.to_be_bytes());
        bytes.extend_from_slice(&self.header.to_be_bytes());

        bytes
    }

    fn from_be_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let message_size = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let header = RequestHeaderV2::from_be_bytes(&bytes[4..])?;

        Ok(RequestV0 {
            message_size,
            header,
        })
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").expect("unable to bind to port");

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut buf = [0; 1024]; // Buffer to hold incoming data

                match stream.read(&mut buf) {
                    Ok(0) => {
                        println!("connection closed by client");
                        continue;
                    }
                    Ok(n) => {
                        println!("received {} bytes from client", n);

                        let request = match RequestV0::from_be_bytes(&buf[..n]) {
                            Ok(req) => req,
                            Err(err) => {
                                eprintln!("error parsing request: {}", err);
                                continue;
                            }
                        };

                        println!("parsed request: {:?}", request);
                        println!("request as bytes: {:?}", request.to_be_bytes());

                        stream
                            .write_all(&request.message_size.to_be_bytes())
                            .expect("unable to write msg_size to stream");
                        stream
                            .write_all(&request.header.correlation_id.to_be_bytes())
                            .expect("unable to write correlation_id to stream");

                        // stream
                        //     .write_all(&request.to_be_bytes())
                        //     .expect("unable to write to stream");
                        stream.flush().expect("unable to flush stream");
                    }
                    Err(e) => {
                        eprintln!("error reading from stream: {}", e);
                        continue;
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
