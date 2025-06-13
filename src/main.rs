use std::{
    io::{Read, Write},
    net::TcpListener,
};

use anyhow::Error;

#[derive(Debug)]
struct RequestHeaderV2 {
    request_api_key: i16,
    request_api_version: i16,
    correlation_id: i32,
    client_id: String,
    tag: String,
}

impl RequestHeaderV2 {
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.request_api_key.to_be_bytes());
        bytes.extend_from_slice(&self.request_api_version.to_be_bytes());
        bytes.extend_from_slice(&self.correlation_id.to_be_bytes());

        if self.client_id.is_empty() {
            bytes.extend_from_slice(&((-1_i16).to_be_bytes())); // -1 for empty client_id
        } else {
            bytes.extend_from_slice(&((self.client_id.len() as i16).to_be_bytes()));
            bytes.extend_from_slice(self.client_id.as_bytes());
        }

        if self.tag.is_empty() {
            bytes.extend_from_slice(&((-1_i16).to_be_bytes())); // -1 for empty tag
        } else {
            bytes.extend_from_slice(&((self.tag.len() as i16).to_be_bytes()));
            bytes.extend_from_slice(self.tag.as_bytes());
        }

        bytes
    }

    fn from_be_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let request_api_key = i16::from_be_bytes([bytes[0], bytes[1]]);
        let request_api_version = i16::from_be_bytes([bytes[2], bytes[3]]);
        let correlation_id = i32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);

        let client_id_length = match i16::from_be_bytes([bytes[8], bytes[9]]) {
            ..-1 => 0,
            n => n as usize,
        };
        let client_id_start = 10;
        let client_id_end = client_id_start + client_id_length;
        let client_id = if client_id_length == 0 {
            String::new()
        } else {
            String::from_utf8(bytes[client_id_start..client_id_end].to_vec())?
        };

        let tag_length_start = client_id_end;
        let tag_length =
            match i16::from_be_bytes([bytes[tag_length_start], bytes[tag_length_start + 1]]) {
                ..-1 => 0,
                n => n as usize,
            };
        let tag_start = tag_length_start + 2;
        let tag_end = tag_start + tag_length;
        let tag = if tag_length == 0 {
            String::new()
        } else {
            String::from_utf8(bytes[tag_start..tag_end].to_vec())?
        };

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
